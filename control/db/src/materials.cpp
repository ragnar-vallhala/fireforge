// MaterialStore: materials, structures and calculations (docs/design/phase8.md §2-§3).
#include "materials.hpp"

#include <algorithm>
#include <cmath>
#include <format>
#include <numeric>
#include <set>

#include <boost/json.hpp>

#include "util.hpp"

namespace tdr::cpdb {

namespace {

using detail::clamp_limit;
using detail::from_json;
using detail::short_id;
using detail::to_json;
using pg::Param;
using pg::Result;
using pg::transact;

#define TS TDR_TS
#define MS TDR_MS

Param opt(const std::string& s) { return s.empty() ? Param(std::nullopt) : Param(s); }

std::string evidence_name(tv1::Evidence e) { return tv1::Evidence_Name(e); }

tv1::Evidence parse_evidence(const std::string& s) {
    tv1::Evidence e = tv1::PREDICTED;
    if (!tv1::Evidence_Parse(s, &e)) throw DbError("unknown evidence '" + s + "'");
    return e;
}

std::string json_array(const std::vector<std::string>& v) {
    boost::json::array a;
    for (const auto& s : v) a.emplace_back(s);
    return boost::json::serialize(a);
}

std::vector<std::string> parse_string_array(const std::string& s) {
    std::vector<std::string> out;
    boost::system::error_code ec;
    auto v = boost::json::parse(s, ec);
    if (ec || !v.is_array()) throw DbError("stored array does not parse: " + s);
    for (const auto& x : v.as_array())
        if (x.is_string()) out.emplace_back(x.as_string());
    return out;
}

// "" when s is empty or a JSON object; otherwise why not.
std::string check_object(const char* field, const std::string& s) {
    if (s.empty()) return {};
    boost::system::error_code ec;
    auto v = boost::json::parse(s, ec);
    if (ec) return std::format("{} is not valid JSON: {}", field, ec.message());
    if (!v.is_object()) return std::format("{} must be a JSON object", field);
    return {};
}

std::string object_or_empty(const std::string& s) { return s.empty() ? "{}" : s; }

// Element symbol: one capital letter, then up to two lowercase ones.
bool valid_species(const std::string& s) {
    if (s.empty() || s.size() > 3 || !(s[0] >= 'A' && s[0] <= 'Z')) return false;
    return std::all_of(s.begin() + 1, s.end(), [](char c) { return c >= 'a' && c <= 'z'; });
}

std::string check_structure(const tv1::Structure& s) {
    const auto& fp = s.fingerprint();
    if (!fp.starts_with("fp1:") || fp.size() == 4) return "structure.fingerprint must be \"fp1:<hash>\"";
    if (fp.size() > 128) return "structure.fingerprint is longer than 128 characters";
    if (s.lattice_size() != 9) return std::format("structure.lattice must have 9 numbers, got {}", s.lattice_size());
    for (double x : s.lattice())
        if (!std::isfinite(x)) return "structure.lattice must be finite";
    if (s.sites().empty()) return "structure.sites must not be empty";
    for (int i = 0; i < s.sites_size(); ++i) {
        const auto& site = s.sites(i);
        if (!valid_species(site.species()))
            return std::format("structure.sites[{}].species '{}' is not an element symbol", i, site.species());
        if (site.frac_size() != 3) return std::format("structure.sites[{}].frac must have 3 numbers", i);
        for (double x : site.frac())
            if (!std::isfinite(x)) return std::format("structure.sites[{}].frac must be finite", i);
    }
    if (!s.magmoms().empty() && s.magmoms_size() != s.sites_size())
        return "structure.magmoms must have one value per site";
    if (s.reduced_formula().size() > 256) return "structure.reduced_formula is longer than 256 characters";
    return {};
}

// Species in order of first appearance, counts divided by their gcd, "1" omitted
// (only used when the structure has no reduced_formula).
std::string derive_reduced_formula(const tv1::Structure& s) {
    std::vector<std::pair<std::string, long>> counts;
    for (const auto& site : s.sites()) {
        auto it = std::find_if(counts.begin(), counts.end(), [&](const auto& p) { return p.first == site.species(); });
        if (it == counts.end()) counts.emplace_back(site.species(), 1);
        else ++it->second;
    }
    long g = 0;
    for (const auto& [_, n] : counts) g = std::gcd(g, n);
    std::string out;
    for (const auto& [el, n] : counts) out += n / g == 1 ? el : el + std::to_string(n / g);
    return out;
}

std::string reduced_formula_of(const tv1::Structure& s) {
    return s.reduced_formula().empty() ? derive_reduced_formula(s) : s.reduced_formula();
}

std::vector<std::string> elements_of(const tv1::Structure& s) {
    std::set<std::string> e;
    for (const auto& site : s.sites()) e.insert(site.species());
    return {e.begin(), e.end()};
}

constexpr const char* kCalcCols =
    "c.calc_id, c.structure_id, c.material_id, m.reduced_formula, c.kind, c.properties::text, c.conditions::text,"
    " c.method::text, c.convergence::text, c.uncertainty::text, c.evidence, c.output_artifacts::text,"
    " to_jsonb(c.input_artifacts)::text, coalesce(c.job_id, ''), coalesce(c.attempt_id, ''),"
    " coalesce(c.workflow_id, ''), coalesce(c.step, ''), coalesce(c.campaign_id, ''), coalesce(c.node_id, ''),"
    " c.note, " MS("c.created_at");

constexpr const char* kCalcFrom = " FROM calculations c JOIN materials m ON m.material_id = c.material_id ";

cv1::Calculation calc_from(const Result& r, int row) {
    cv1::Calculation c;
    c.set_calc_id(r.str(row, 0));
    c.set_structure_id(r.str(row, 1));
    c.set_material_id(r.str(row, 2));
    c.set_reduced_formula(r.str(row, 3));
    c.set_kind(r.str(row, 4));
    c.set_properties_json(r.str(row, 5));
    c.set_conditions_json(r.str(row, 6));
    from_json(r.str(row, 7), c.mutable_method());
    c.set_convergence_json(r.str(row, 8));
    c.set_uncertainty_json(r.str(row, 9));
    c.set_evidence(parse_evidence(r.str(row, 10)));
    boost::system::error_code ec;
    auto outs = boost::json::parse(r.str(row, 11), ec);
    if (ec || !outs.is_object()) throw DbError("stored output_artifacts do not parse");
    std::vector<std::pair<std::string, std::string>> named;
    for (const auto& kv : outs.as_object())
        if (kv.value().is_string()) named.emplace_back(std::string(kv.key()), std::string(kv.value().as_string()));
    std::sort(named.begin(), named.end());
    for (auto& [name, id] : named) {
        auto* o = c.add_outputs();
        o->set_name(name);
        o->set_artifact_id(id);
    }
    for (auto& a : parse_string_array(r.str(row, 12))) c.add_input_artifacts(std::move(a));
    c.set_job_id(r.str(row, 13));
    c.set_attempt_id(r.str(row, 14));
    c.set_workflow_id(r.str(row, 15));
    c.set_step(r.str(row, 16));
    c.set_campaign_id(r.str(row, 17));
    c.set_node_id(r.str(row, 18));
    c.set_note(r.str(row, 19));
    c.set_created_unix_ms(r.i64(row, 20));
    return c;
}

constexpr const char* kMaterialCols =
    "m.material_id, m.reduced_formula, to_jsonb(m.elements)::text, " MS("m.created_at") ","
    " (SELECT count(*) FROM structures s WHERE s.material_id = m.material_id),"
    " (SELECT count(*) FROM calculations c WHERE c.material_id = m.material_id)";

cv1::Material material_from(const Result& r, int row) {
    cv1::Material m;
    m.set_material_id(r.str(row, 0));
    m.set_reduced_formula(r.str(row, 1));
    for (auto& e : parse_string_array(r.str(row, 2))) m.add_elements(std::move(e));
    m.set_created_unix_ms(r.i64(row, 3));
    m.set_structure_count(static_cast<std::uint32_t>(r.u64(row, 4)));
    m.set_calculation_count(static_cast<std::uint32_t>(r.u64(row, 5)));
    return m;
}

constexpr const char* kStructureCols =
    "s.structure_id, s.material_id, s.structure::text, coalesce(s.source_job_id, ''), " MS("s.created_at");

cv1::StructureInfo structure_from(const Result& r, int row) {
    cv1::StructureInfo s;
    s.set_structure_id(r.str(row, 0));
    s.set_material_id(r.str(row, 1));
    from_json(r.str(row, 2), s.mutable_structure());
    s.set_source_job_id(r.str(row, 3));
    s.set_created_unix_ms(r.i64(row, 4));
    return s;
}

}  // namespace

// ---- reads ------------------------------------------------------------------------

std::vector<cv1::Material> MaterialStore::list_materials(const std::string& formula, const std::string& element,
                                                         const std::string& campaign_id, std::size_t limit) {
    return c_.retry([&] {
        auto r = c_.exec(std::string("SELECT ") + kMaterialCols +
                             " FROM materials m WHERE ($1 = '' OR m.reduced_formula = $1)"
                             " AND ($2 = '' OR m.elements @> ARRAY[$2]::text[])"
                             " AND ($3 = '' OR EXISTS (SELECT 1 FROM calculations c"
                             "   WHERE c.material_id = m.material_id AND c.campaign_id = $3))"
                             " ORDER BY m.reduced_formula LIMIT $4::bigint",
                         formula, element, campaign_id, clamp_limit(std::min<std::size_t>(limit, 1000)));
        std::vector<cv1::Material> out;
        for (int i = 0; i < r.rows(); ++i) out.push_back(material_from(r, i));
        return out;
    });
}

std::optional<cv1::Material> MaterialStore::get_material(const std::string& material_id) {
    return c_.retry([&]() -> std::optional<cv1::Material> {
        auto r = c_.exec(std::string("SELECT ") + kMaterialCols + " FROM materials m WHERE m.material_id = $1",
                         material_id);
        if (r.rows() != 1) return std::nullopt;
        auto m = material_from(r, 0);
        auto s = c_.exec(std::string("SELECT ") + kStructureCols +
                             " FROM structures s WHERE s.material_id = $1 ORDER BY s.seq",
                         material_id);
        for (int i = 0; i < s.rows(); ++i) *m.add_structures() = structure_from(s, i);
        auto c = c_.exec(std::string("SELECT DISTINCT ON (c.kind) ") + kCalcCols + kCalcFrom +
                             "WHERE c.material_id = $1 ORDER BY c.kind, c.seq DESC",
                         material_id);
        for (int i = 0; i < c.rows(); ++i) *m.add_latest() = calc_from(c, i);
        return m;
    });
}

std::optional<cv1::StructureInfo> MaterialStore::get_structure(const std::string& structure_id) {
    return c_.retry([&]() -> std::optional<cv1::StructureInfo> {
        auto r = c_.exec(std::string("SELECT ") + kStructureCols + " FROM structures s WHERE s.structure_id = $1",
                         structure_id);
        if (r.rows() != 1) return std::nullopt;
        return structure_from(r, 0);
    });
}

cv1::CalculationList MaterialStore::list_calculations(const IRepository::CalcQuery& q) {
    const std::size_t limit = std::clamp<std::size_t>(q.limit, 1, 1000);
    const std::optional<std::string> ev = q.evidence ? std::optional(evidence_name(*q.evidence)) : std::nullopt;
    return c_.retry([&] {
        // Newest first by insertion order; `after` resumes below that calculation.
        auto r = c_.exec(std::string("SELECT ") + kCalcCols + kCalcFrom +
                             "WHERE ($1 = '' OR c.material_id = $1) AND ($2 = '' OR c.structure_id = $2)"
                             " AND ($3 = '' OR c.kind = $3) AND ($4 = '' OR c.campaign_id = $4)"
                             " AND ($5 = '' OR c.workflow_id = $5) AND ($6::text IS NULL OR c.evidence = $6)"
                             " AND ($7 = '' OR c.seq < (SELECT seq FROM calculations WHERE calc_id = $7))"
                             " ORDER BY c.seq DESC LIMIT $8::bigint",
                         q.material_id, q.structure_id, q.kind, q.campaign_id, q.workflow_id, ev, q.after,
                         clamp_limit(limit + 1));
        cv1::CalculationList out;
        const int n = std::min(r.rows(), static_cast<int>(limit));
        for (int i = 0; i < n; ++i) *out.add_calculations() = calc_from(r, i);
        if (r.rows() > n && n > 0) out.set_next(out.calculations(n - 1).calc_id());
        return out;
    });
}

std::optional<cv1::Calculation> MaterialStore::get_calculation(const std::string& calc_id) {
    return c_.retry([&] { return load_calculation(calc_id); });
}

std::optional<cv1::Calculation> MaterialStore::load_calculation(const std::string& calc_id) {
    auto r = c_.exec(std::string("SELECT ") + kCalcCols + kCalcFrom + "WHERE c.calc_id = $1", calc_id);
    if (r.rows() != 1) return std::nullopt;
    return calc_from(r, 0);
}

// ---- import -------------------------------------------------------------------------

std::expected<cv1::ImportCalculationsReply, std::string> MaterialStore::import_calculations(
    const cv1::ImportCalculationsRequest& req, const std::string& actor, std::int64_t now_ms) {
    if (req.records().empty()) return std::unexpected("records must not be empty");
    return transact(c_, [&]() -> std::expected<cv1::ImportCalculationsReply, std::string> {
        Context ctx;
        ctx.import = true;
        ctx.campaign_id = req.campaign_id();
        if (!ctx.campaign_id.empty() &&
            c_.exec("SELECT 1 FROM campaigns WHERE campaign_id = $1", ctx.campaign_id).rows() != 1)
            return std::unexpected("unknown campaign " + ctx.campaign_id);
        if (auto err = validate(req.records(), ctx); !err.empty()) return std::unexpected(err);
        cv1::ImportCalculationsReply out;
        for (auto& id : store(req.records(), ctx, now_ms)) out.add_calc_ids(std::move(id));
        boost::json::object details;
        details["records"] = req.records_size();
        details["campaign_id"] = req.campaign_id();
        c_.exec("INSERT INTO audit_log (at, actor, action, target, details) VALUES (" TS(1) ", $2, $3, $4, $5::jsonb)",
                now_ms, actor, "calculations.import", req.campaign_id(), boost::json::serialize(details));
        return out;
    });
}

// ---- validate + store (caller's transaction) ------------------------------------------

std::string MaterialStore::validate(const Records& records, const Context& ctx) {
    if (records.size() > kMaxRecords)
        return std::format("{} records exceed the maximum of {} per result", records.size(), kMaxRecords);
    std::size_t bytes = 0;
    for (const auto& r : records) bytes += r.ByteSizeLong();
    if (bytes > kMaxRecordBytes)
        return std::format("records are {} bytes, more than the maximum of {} per result", bytes, kMaxRecordBytes);
    for (int i = 0; i < records.size(); ++i) {
        const auto& r = records[i];
        auto bad = [&](const std::string& why) { return std::format("record {} ({}): {}", i, r.kind(), why); };
        if (r.kind().empty()) return bad("kind is required");
        if (r.kind().size() > kMaxKindChars) return bad(std::format("kind is longer than {} characters", kMaxKindChars));
        if (ctx.import) {
            if (r.evidence() != tv1::LITERATURE && r.evidence() != tv1::MEASURED)
                return bad("imported records must have evidence LITERATURE or MEASURED");
            if (!r.output_names().empty()) return bad("imported records must not have output_names");
        } else if (r.evidence() != tv1::PREDICTED) {
            return bad("worker records must have evidence PREDICTED");
        }
        if (!r.structure_id().empty()) {
            if (c_.exec("SELECT 1 FROM structures WHERE structure_id = $1", r.structure_id()).rows() != 1)
                return bad("unknown structure " + r.structure_id());
        } else if (r.has_structure()) {
            if (auto e = check_structure(r.structure()); !e.empty()) return bad(e);
        } else {
            return bad("structure_id or structure is required");
        }
        for (auto [field, value] : {std::pair{"properties_json", &r.properties_json()},
                                    std::pair{"conditions_json", &r.conditions_json()},
                                    std::pair{"convergence_json", &r.convergence_json()},
                                    std::pair{"uncertainty_json", &r.uncertainty_json()},
                                    std::pair{"method.settings_json", &r.method().settings_json()}})
            if (auto e = check_object(field, *value); !e.empty()) return bad(e);
        for (const auto& name : r.output_names())
            if (!ctx.outputs.contains(name)) return bad(std::format("output_names: '{}' is not an output of this result", name));
    }
    return {};
}

std::vector<std::string> MaterialStore::store(const Records& records, const Context& ctx, std::int64_t now_ms) {
    // 1. Inline structures: the ones whose fingerprint is already known.
    std::map<std::string, std::pair<std::string, std::string>> by_fp;  // fingerprint -> (structure_id, material_id)
    std::map<std::string, const tv1::Structure*> new_fp;                // first inline structure per new fingerprint
    for (const auto& r : records) {
        if (!r.structure_id().empty() || by_fp.contains(r.structure().fingerprint()) ||
            new_fp.contains(r.structure().fingerprint()))
            continue;
        const auto& fp = r.structure().fingerprint();
        auto s = c_.exec("SELECT structure_id, material_id FROM structures WHERE fingerprint = $1", fp);
        if (s.rows() == 1) by_fp[fp] = {s.str(0, 0), s.str(0, 1)};
        else new_fp[fp] = &r.structure();
    }
    // 2. Materials for the new structures, sorted by formula.
    std::map<std::string, std::pair<std::vector<std::string>, int>> formulas;  // formula -> (elements, nsites)
    for (const auto& [_, st] : new_fp) {
        auto [it, fresh] = formulas.try_emplace(reduced_formula_of(*st), elements_of(*st), st->sites_size());
        if (!fresh) it->second.second = std::min(it->second.second, st->sites_size());
    }
    std::map<std::string, std::string> material_of;  // formula -> material_id
    for (const auto& [formula, info] : formulas) {
        auto ins = c_.exec("INSERT INTO materials (material_id, reduced_formula, elements, nsites_min, created_at)"
                           " VALUES ($1, $2, ARRAY(SELECT jsonb_array_elements_text($3::jsonb)), $4::int, " TS(5) ")"
                           " ON CONFLICT (reduced_formula) DO NOTHING RETURNING material_id",
                           short_id("mat-"), formula, json_array(info.first), info.second, now_ms);
        if (ins.rows() == 1) {
            material_of[formula] = ins.str(0, 0);
            continue;
        }
        auto m = c_.exec("SELECT material_id FROM materials WHERE reduced_formula = $1", formula);
        if (m.rows() != 1) throw DbError("material " + formula + " vanished during upsert");
        material_of[formula] = m.str(0, 0);
    }
    // 3. New structures, sorted by fingerprint (a concurrent insert of the same one wins; we use it).
    for (const auto& [fp, st] : new_fp) {
        tv1::Structure stored = *st;
        stored.set_reduced_formula(reduced_formula_of(*st));
        const std::string& mid = material_of.at(stored.reduced_formula());
        auto ins = c_.exec(
            "INSERT INTO structures (structure_id, material_id, fingerprint, structure, spacegroup_number, prototype,"
            " source_job_id, parent_structure_id, created_at)"
            " VALUES ($1, $2, $3, $4::jsonb, $5::int, $6, $7, $8, " TS(9) ")"
            " ON CONFLICT (fingerprint) DO NOTHING RETURNING structure_id",
            short_id("st-"), mid, fp, to_json(stored), stored.spacegroup_number(), stored.prototype(),
            opt(ctx.job_id), opt(stored.parent_structure_id()), now_ms);
        if (ins.rows() == 1) {
            by_fp[fp] = {ins.str(0, 0), mid};
            c_.exec("UPDATE materials SET nsites_min = CASE WHEN nsites_min = 0 THEN $2::int"
                    " ELSE least(nsites_min, $2::int) END WHERE material_id = $1",
                    mid, st->sites_size());
            continue;
        }
        auto s = c_.exec("SELECT structure_id, material_id FROM structures WHERE fingerprint = $1", fp);
        if (s.rows() != 1) throw DbError("structure " + fp + " vanished during upsert");
        by_fp[fp] = {s.str(0, 0), s.str(0, 1)};
    }
    // 4. Calculations, in record order.
    std::vector<std::string> ids;
    for (const auto& r : records) {
        std::string sid, mid;
        if (!r.structure_id().empty()) {
            auto s = c_.exec("SELECT material_id FROM structures WHERE structure_id = $1", r.structure_id());
            if (s.rows() != 1) throw DbError("structure " + r.structure_id() + " vanished");
            sid = r.structure_id();
            mid = s.str(0, 0);
        } else {
            std::tie(sid, mid) = by_fp.at(r.structure().fingerprint());
        }
        boost::json::object outs;
        for (const auto& name : r.output_names()) outs[name] = ctx.outputs.at(name);
        std::vector<std::string> inputs(r.input_artifacts().begin(), r.input_artifacts().end());
        const std::string id = short_id("calc-");
        c_.exec("INSERT INTO calculations (calc_id, structure_id, material_id, kind, properties, conditions, method,"
                " convergence, uncertainty, evidence, output_artifacts, input_artifacts, job_id, attempt_id,"
                " workflow_id, step, campaign_id, node_id, note, created_at)"
                " VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, $8::jsonb, $9::jsonb, $10, $11::jsonb,"
                " ARRAY(SELECT jsonb_array_elements_text($12::jsonb)), $13, $14, $15, $16, $17, $18, $19, " TS(20) ")",
                id, sid, mid, r.kind(), object_or_empty(r.properties_json()), object_or_empty(r.conditions_json()),
                to_json(r.method()), object_or_empty(r.convergence_json()), object_or_empty(r.uncertainty_json()),
                evidence_name(r.evidence()), boost::json::serialize(outs), json_array(inputs), opt(ctx.job_id),
                opt(ctx.attempt_id), opt(ctx.workflow_id), opt(ctx.step), opt(ctx.campaign_id), opt(ctx.node_id),
                r.note(), now_ms);
        ids.push_back(id);
    }
    return ids;
}

#undef TS
#undef MS

}  // namespace tdr::cpdb
