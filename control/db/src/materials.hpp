// Materials database on PostgreSQL (docs/design/phase8.md §2-§3). Internal:
// PgRepository forwards the IRepository materials methods here, and JobStore
// stores a COMPLETED update's records through validate() + store() inside its
// own transaction.
//
// Lock order inside store(): materials rows (by reduced_formula) before
// structures rows (by fingerprint), each set in sorted order, so two
// transactions inserting overlapping sets cannot deadlock on the unique indexes.
#pragma once

#include <map>
#include <string>
#include <vector>

#include "pg.hpp"
#include "tdr/cpdb/db.hpp"

namespace tdr::cpdb {

class MaterialStore {
public:
    static constexpr int kMaxRecords = 64;
    static constexpr std::size_t kMaxRecordBytes = 256 * 1024;  // serialized CalcRecord bytes per update
    static constexpr std::size_t kMaxKindChars = 64;

    using Records = google::protobuf::RepeatedPtrField<tv1::CalcRecord>;

    // Where the records come from (columns of every calculation stored).
    struct Context {
        bool import = false;  // admin import: LITERATURE/MEASURED, no output_names
        std::string job_id, attempt_id, workflow_id, step, campaign_id, node_id;
        std::map<std::string, std::string> outputs;  // this update's named outputs: name -> artifact id
    };

    explicit MaterialStore(pg::Conn& c) : c_(c) {}

    std::vector<cv1::Material> list_materials(const std::string& formula, const std::string& element,
                                              const std::string& campaign_id, std::size_t limit);
    std::optional<cv1::Material> get_material(const std::string& material_id);
    std::optional<cv1::StructureInfo> get_structure(const std::string& structure_id);
    cv1::CalculationList list_calculations(const IRepository::CalcQuery& q);
    std::optional<cv1::Calculation> get_calculation(const std::string& calc_id);
    std::expected<cv1::ImportCalculationsReply, std::string> import_calculations(
        const cv1::ImportCalculationsRequest& req, const std::string& actor, std::int64_t now_ms);

    // ---- inside the caller's transaction ----------------------------------------
    // "" = every record is valid (reads only: structure_id existence).
    std::string validate(const Records& records, const Context& ctx);
    // Stores validated records; returns the new calc ids in record order.
    std::vector<std::string> store(const Records& records, const Context& ctx, std::int64_t now_ms);

private:
    std::optional<cv1::Calculation> load_calculation(const std::string& calc_id);

    pg::Conn& c_;
};

}  // namespace tdr::cpdb
