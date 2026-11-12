#pragma once

// MQService's own database login.
//
// A custom process is handed one pool, built from postgres.helper, and
// Application::db_pool(role) knows no name but worker, helper and kernel. The
// database may admit the exchange only under a role of its own, so MQService
// opens a pool of its own, and this header builds its connection string.
//
// Where the connection goes is the helper pool's: same host, port, database
// and TLS — directly to the server, which a LISTEN needs (a transaction pooler
// drops it). Who connects is the section postgres.exchange: "user" and
// "password", nothing else, both from the configuration only.
// AppSettings::build_pg_conninfo is not used: there PGUSER and PGPASSWORD from
// the environment win for every role that has no variables of its own, so a
// process started with PGUSER set would sign in as that user and no line would
// say so.
//
// Refused at start, never retried: no helper connection to take the address
// from, a section that is absent or not an object, a key other than
// user/password, an empty value (an unset ${VAR} is expanded to an empty
// string), a user with a blank, a control or a non-ASCII character, a password
// with a control character, and a user that is the role of another pool of the
// process (worker, helper, kernel; a pool that names no user connects as
// libpq's default user, and that one is compared) — each would leave the
// process alive and refused by the database, or connected under a role that is
// not its own.
//
// Free of Application and PgPool, so that it can be tested on its own.

#include <libpq-fe.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <cctype>
#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace apostol::mqservice
{

namespace own_pool_detail
{

inline std::string missing(std::string_view where, const char* key)
{
    return std::string(where) + "." + key + " is not set";
}

inline bool printable(std::string_view s) noexcept
{
    return std::all_of(s.begin(), s.end(), [](char c) { return std::isgraph(static_cast<unsigned char>(c)); });
}

inline bool blank(std::string_view s) noexcept
{
    return std::all_of(s.begin(), s.end(), [](char c) { return std::isspace(static_cast<unsigned char>(c)); });
}

// libpq keyword=value: the value in single quotes, ' and \ escaped with a backslash.
inline void append(std::string& out, std::string_view key, std::string_view value)
{
    if (!out.empty())
        out += ' ';
    out += key;
    out += "='";
    for (char c : value) {
        if (c == '\'' || c == '\\')
            out += '\\';
        out += c;
    }
    out += '\'';
}

inline std::string value_of(const nlohmann::json& section, const char* key, std::string_view where)
{
    const auto it = section.find(key);
    if (it == section.end() || it->is_null())
        throw std::runtime_error(missing(where, key));
    if (!it->is_string())
        throw std::runtime_error(std::string(where) + "." + key + " is not a string");
    return it->get<std::string>();
}

using Options = std::unique_ptr<PQconninfoOption, decltype(&PQconninfoFree)>;

// libpq's error text quotes the string it failed on, a password with it — never repeated.
inline Options parse(const std::string& conninfo)
{
    char* err = nullptr;
    Options opts(PQconninfoParse(conninfo.c_str(), &err), &PQconninfoFree);
    if (err)
        PQfreemem(err);
    return opts;
}

} // namespace own_pool_detail

/// The user a libpq conninfo connects as: the one it names, else libpq's
/// default (PGUSER, then the operating-system user). Empty when the string
/// does not parse or no default can be found.
inline std::string user_of(const std::string& conninfo)
{
    const auto opts = own_pool_detail::parse(conninfo);
    if (!opts)
        return {};
    for (const PQconninfoOption* o = opts.get(); o->keyword; ++o)
        if (std::string_view(o->keyword) == "user" && o->val && *o->val)
            return o->val;
    own_pool_detail::Options defaults(PQconndefaults(), &PQconninfoFree);
    if (defaults)
        for (const PQconninfoOption* o = defaults.get(); o->keyword; ++o)
            if (std::string_view(o->keyword) == "user" && o->val)
                return o->val;
    return {};
}

/// The connection string of the process's own pool: every parameter of
/// @p base (the helper pool's resolved conninfo) except user and password,
/// which come from @p section. @p others — the resolved conninfos of the
/// process's other pools (worker, kernel): their roles are refused, the
/// helper's always. Throws std::runtime_error naming the key on any refusal;
/// @p where names the section in that text.
inline std::string own_conninfo(const std::string& base, const nlohmann::json& section, std::string_view where,
                                const std::vector<std::string>& others = {})
{
    using namespace own_pool_detail;

    if (blank(base))
        throw std::runtime_error("no helper connection (postgres.helper) to take the address from");
    if (!section.is_object())
        throw std::runtime_error(std::string(where) + " is not set (an object with \"user\" and \"password\")");

    for (const auto& [key, _] : section.items())
        if (key != "user" && key != "password" && key != "_comment")
            throw std::runtime_error(std::string(where) + "." + key +
                                     ": not a key of this section — only user and password are read; "
                                     "the rest is the helper pool's");

    const auto user     = value_of(section, "user", where);
    const auto password = value_of(section, "password", where);

    if (blank(user))
        throw std::runtime_error(missing(where, "user"));
    if (blank(password))
        throw std::runtime_error(missing(where, "password") + " (the variable behind it was empty)");
    if (!printable(user))
        throw std::runtime_error(std::string(where) + ".user has a blank, a control or a non-ASCII character");
    if (std::any_of(password.begin(), password.end(), [](char c) { return std::iscntrl(static_cast<unsigned char>(c)); }))
        throw std::runtime_error(std::string(where) + ".password has a control character");

    std::vector<std::string> taken{user_of(base)};
    for (const auto& c : others)
        if (!c.empty())
            taken.push_back(user_of(c));
    if (std::find(taken.begin(), taken.end(), user) != taken.end())
        throw std::runtime_error(std::string(where) + ".user is '" + user +
                                 "', the role of another pool of this process — it needs a role of its own");

    const auto opts = parse(base);
    if (!opts)
        throw std::runtime_error("the helper pool's connection settings do not parse");

    std::string out;
    for (const PQconninfoOption* o = opts.get(); o->keyword; ++o) {
        if (!o->val || !*o->val)
            continue;
        const std::string_view key = o->keyword;
        if (key == "user" || key == "password")
            continue;
        append(out, key, o->val);
    }
    append(out, "user", user);
    append(out, "password", password);
    return out;
}

} // namespace apostol::mqservice
