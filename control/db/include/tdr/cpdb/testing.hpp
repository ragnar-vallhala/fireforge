// Throwaway PostgreSQL clusters for tests (no sudo, Unix socket only).
#pragma once

#include <filesystem>
#include <string>

namespace tdr::cpdb::testing {

// initdb into a fresh temp dir under /tmp, start with pg_ctl
// (listen_addresses='', unix_socket_directories=<dir>, shared_buffers=16MB,
// fsync=off), create database "fireforge". Destructor stops the server and
// deletes the dir. Binaries: $PG_BIN, else /usr/lib/postgresql/16/bin, else
// pg_config --bindir. Throws std::runtime_error with initdb/pg_ctl output on failure.
class TempCluster {
public:
    TempCluster();
    ~TempCluster();
    TempCluster(const TempCluster&) = delete;
    TempCluster& operator=(const TempCluster&) = delete;

    // e.g. "host=/tmp/ffpg-XXXX port=5432 dbname=fireforge"
    const std::string& conninfo() const { return conninfo_; }
    // A fresh empty database on this cluster (CREATE DATABASE <name>), returning its conninfo.
    std::string create_database(const std::string& name);

private:
    std::filesystem::path dir_;
    std::filesystem::path bin_;
    std::string conninfo_;
};

}  // namespace tdr::cpdb::testing
