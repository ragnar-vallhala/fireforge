#include "pg.hpp"

#include <charconv>
#include <format>

namespace tdr::cpdb::pg {

namespace {

template <class T>
T parse_int(const std::string& s, int row, int col) {
    T v{};
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    if (ec != std::errc{} || p != s.data() + s.size())
        throw DbError(std::format("bad integer '{}' at row {} col {}", s, row, col));
    return v;
}

std::string trim(std::string s) {
    while (!s.empty() && (s.back() == '\n' || s.back() == ' ')) s.pop_back();
    return s;
}

}  // namespace

std::int64_t Result::i64(int row, int col) const {
    if (null(row, col)) return 0;
    return parse_int<std::int64_t>(str(row, col), row, col);
}

std::uint64_t Result::u64(int row, int col) const {
    if (null(row, col)) return 0;
    return parse_int<std::uint64_t>(str(row, col), row, col);
}

std::int64_t Result::affected() const {
    const char* n = PQcmdTuples(r_.get());
    if (!n || !*n) return 0;
    std::int64_t v = 0;
    std::from_chars(n, n + std::char_traits<char>::length(n), v);
    return v;
}

Conn::Conn(const std::string& conninfo) : c_(PQconnectdb(conninfo.c_str()), &PQfinish) {
    if (!c_) throw DbError("libpq: out of memory");
    if (PQstatus(c_.get()) != CONNECTION_OK)
        throw DbError("cannot connect to postgres: " + trim(PQerrorMessage(c_.get())), "08001");
    // Keep server notices (e.g. "relation already exists, skipping") off stderr.
    PQsetNoticeProcessor(c_.get(), [](void*, const char*) {}, nullptr);
}

void Conn::reset() {
    PQreset(c_.get());
    if (PQstatus(c_.get()) != CONNECTION_OK)
        throw ConnectionLost("postgres reconnect failed: " + trim(PQerrorMessage(c_.get())), "08006");
}

void Conn::fail(PGresult* r, std::string_view what) {
    std::string state, msg;
    if (r) {
        if (const char* s = PQresultErrorField(r, PG_DIAG_SQLSTATE)) state = s;
        msg = trim(PQresultErrorMessage(r));
        PQclear(r);
    }
    if (msg.empty()) msg = trim(PQerrorMessage(c_.get()));
    std::string full = std::format("postgres {} failed [{}]: {}", what, state.empty() ? "?" : state, msg);
    if (bad()) throw ConnectionLost(full, state.empty() ? "08006" : state);
    throw DbError(full, state);
}

Result Conn::exec_v(std::string_view sql, const std::vector<Param>& params) {
    if (bad()) reset();
    std::vector<const char*> values;
    values.reserve(params.size());
    for (const auto& p : params) values.push_back(p.v ? p.v->c_str() : nullptr);
    std::string q(sql);
    PGresult* r = PQexecParams(c_.get(), q.c_str(), static_cast<int>(values.size()), nullptr, values.data(), nullptr,
                               nullptr, 0);
    auto st = r ? PQresultStatus(r) : PGRES_FATAL_ERROR;
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) fail(r, "query");
    return Result(r);
}

void Conn::exec_script(std::string_view sql) {
    if (bad()) reset();
    std::string q(sql);
    PGresult* r = PQexec(c_.get(), q.c_str());
    auto st = r ? PQresultStatus(r) : PGRES_FATAL_ERROR;
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) fail(r, "script");
    PQclear(r);
}

Tx::Tx(Conn& c) : c_(c) { c_.exec("BEGIN"); }

Tx::~Tx() {
    if (done_ || c_.bad()) return;
    try {
        c_.exec("ROLLBACK");
    } catch (...) {
    }
}

void Tx::commit() {
    try {
        c_.exec("COMMIT");
    } catch (const ConnectionLost& e) {
        done_ = true;
        throw DbError(std::string("commit outcome unknown: ") + e.what(), e.sqlstate);
    }
    done_ = true;
}

}  // namespace tdr::cpdb::pg
