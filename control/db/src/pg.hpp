// Internal RAII wrapper around libpq (not a public contract).
//
// Every value goes through PQexecParams as a text parameter; SQL text never
// contains data. A broken connection (server restart, terminated backend)
// surfaces as ConnectionLost; Conn::reset() runs PQreset.
#pragma once

#include <libpq-fe.h>

#include <concepts>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "tdr/cpdb/db.hpp"

namespace tdr::cpdb::pg {

// The connection broke while running a statement. Safe to retry the whole
// unit of work after Conn::reset() (the server rolled back any open tx).
struct ConnectionLost : DbError {
    using DbError::DbError;
};

// A text parameter; nullopt binds SQL NULL.
struct Param {
    std::optional<std::string> v;

    Param(std::nullopt_t) {}
    Param(std::string s) : v(std::move(s)) {}
    Param(std::string_view s) : v(std::string(s)) {}
    Param(const char* s) : v(std::string(s)) {}
    Param(std::optional<std::string> s) : v(std::move(s)) {}
    Param(bool b) : v(b ? "true" : "false") {}
    template <std::integral T>
        requires(!std::same_as<T, bool> && !std::same_as<T, char>)
    Param(T x) : v(std::to_string(x)) {}
};

class Result {
public:
    explicit Result(PGresult* r) : r_(r, &PQclear) {}

    int rows() const { return PQntuples(r_.get()); }
    bool null(int row, int col) const { return PQgetisnull(r_.get(), row, col) != 0; }
    std::string str(int row, int col) const {
        return std::string(PQgetvalue(r_.get(), row, col), static_cast<std::size_t>(PQgetlength(r_.get(), row, col)));
    }
    std::int64_t i64(int row, int col) const;
    std::uint64_t u64(int row, int col) const;
    // Rows touched by INSERT/UPDATE/DELETE.
    std::int64_t affected() const;

private:
    std::unique_ptr<PGresult, decltype(&PQclear)> r_;
};

class Conn {
public:
    // Throws DbError if the server is unreachable.
    explicit Conn(const std::string& conninfo);
    Conn(const Conn&) = delete;
    Conn& operator=(const Conn&) = delete;

    template <class... A>
    Result exec(std::string_view sql, A&&... params) {
        std::vector<Param> ps;
        ps.reserve(sizeof...(A));
        (ps.emplace_back(std::forward<A>(params)), ...);
        return exec_v(sql, ps);
    }
    Result exec_v(std::string_view sql, const std::vector<Param>& params);
    // Multi-statement script without parameters (migrations only).
    void exec_script(std::string_view sql);

    // PQreset; throws ConnectionLost if the server is still unreachable.
    void reset();
    bool bad() const { return PQstatus(c_.get()) == CONNECTION_BAD; }
    PGconn* raw() { return c_.get(); }

    // Runs f() and, if the connection was lost, resets and runs it once more.
    template <class F>
    auto retry(F&& f) -> decltype(f()) {
        try {
            return f();
        } catch (const ConnectionLost&) {
            reset();
            return f();
        }
    }

private:
    [[noreturn]] void fail(PGresult* r, std::string_view what);
    std::unique_ptr<PGconn, decltype(&PQfinish)> c_;
};

// BEGIN in the constructor, ROLLBACK in the destructor unless commit() ran.
class Tx {
public:
    explicit Tx(Conn& c);
    ~Tx();
    Tx(const Tx&) = delete;
    Tx& operator=(const Tx&) = delete;
    // A lost connection during COMMIT is reported as a plain DbError: the
    // outcome is unknown, so the caller must not blindly retry.
    void commit();

private:
    Conn& c_;
    bool done_ = false;
};

// Runs f(conn) inside a transaction, retrying once on a lost connection.
template <class F>
auto transact(Conn& c, F&& f) -> decltype(f()) {
    return c.retry([&]() -> decltype(f()) {
        Tx tx(c);
        if constexpr (std::is_void_v<decltype(f())>) {
            f();
            tx.commit();
        } else {
            auto r = f();
            tx.commit();
            return r;
        }
    });
}

// Applies pending migrations (each in its own transaction, under an advisory lock).
void migrate(Conn& c);
int current_version(Conn& c);
int latest_version();

}  // namespace tdr::cpdb::pg
