"""Structure model: fingerprint invariance/distinctness, conversions (phase8.md §3)."""
from __future__ import annotations

import json

import numpy as np
import pytest

from fireforge_science.model import Structure, canonical_form, load_structure, save_structure

from conftest import ZRNISN_A, fcc_lattice, halfheusler


def rotation(axis, angle):
    axis = np.asarray(axis, float) / np.linalg.norm(axis)
    k = np.array([[0, -axis[2], axis[1]], [axis[2], 0, -axis[0]], [-axis[1], axis[0], 0]])
    return np.eye(3) + np.sin(angle) * k + (1 - np.cos(angle)) * k @ k


def conventional_hh(x="Zr", y="Ni", z="Sn", a=ZRNISN_A) -> Structure:
    fcc = np.array([[0, 0, 0], [0, .5, .5], [.5, 0, .5], [.5, .5, 0]])
    sp, fr = [], []
    for el, shift in ((x, 0.0), (y, 0.25), (z, 0.5)):
        for p in fcc:
            sp.append(el)
            fr.append(p + shift)
    return Structure(np.eye(3) * a, sp, np.array(fr) % 1.0)


def triclinic() -> Structure:
    """A P1 structure (no symmetry to idealize): tests origin/basis canonicalization."""
    return Structure([[4.1, 0.2, 0.1], [0.7, 5.3, -0.2], [0.4, 0.9, 6.2]],
                     ["Ba", "Ti", "O", "O", "O"],
                     [[0.03, 0.07, 0.11], [0.52, 0.47, 0.55], [0.61, 0.13, 0.42],
                      [0.18, 0.66, 0.37], [0.33, 0.29, 0.83]])


# ---- fingerprint -------------------------------------------------------------------------

def test_fingerprint_format(zrnisn):
    fp = zrnisn.fingerprint()
    assert fp.startswith("fp1:") and len(fp) == 4 + 64
    assert fp == halfheusler().fingerprint()        # deterministic


@pytest.mark.parametrize("base", ["hh", "triclinic"])
def test_fingerprint_invariance(base):
    s = halfheusler() if base == "hh" else triclinic()
    fp = s.fingerprint()
    n = s.nsites
    # supercells: diagonal and non-diagonal
    assert s.supercell([2, 2, 2]).fingerprint() == fp
    assert s.supercell([[1, 1, 0], [0, 1, 1], [1, 0, 2]]).fingerprint() == fp
    # site permutation
    perm = np.random.default_rng(3).permutation(n)
    p = Structure(s.lattice, [s.species[i] for i in perm], s.frac_array[perm])
    assert p.fingerprint() == fp
    # rigid translation / origin shift (incl. wrapping)
    for shift in ([0.37, -0.21, 0.05], [0.5, 0.5, 0.5], [-1.25, 2.0, 0.75]):
        t = s.copy(frac=((s.frac_array + shift) % 1.0).tolist())
        assert t.fingerprint() == fp, shift
        assert s.copy(frac=(s.frac_array + shift).tolist()).fingerprint() == fp
    # rotation of the Cartesian frame
    r = rotation([1, 2, 3], 0.7)
    assert s.copy(lattice=(s.lattice_array @ r.T).tolist()).fingerprint() == fp
    # other lattice basis (unimodular change of basis, positions transformed)
    m = np.array([[1, 1, 0], [0, 1, 0], [1, 1, 1]])
    lat2 = m @ s.lattice_array
    frac2 = s.frac_array @ np.linalg.inv(m)
    assert Structure(lat2, s.species, frac2 % 1.0).fingerprint() == fp


def test_fingerprint_conventional_equals_primitive():
    assert conventional_hh().fingerprint() == halfheusler().fingerprint()
    assert conventional_hh().nsites == 12


@pytest.mark.parametrize("seed", range(5))
def test_fingerprint_robust_to_jitter(seed):
    """Noise of 1e-4 Å (positions and lattice) is removed by spglib's idealization, as
    long as the lattice length is not near a 1e-3 Å rounding edge (a = 6.1094: primitive
    length 4.31994 Å, mid-bin)."""
    ref = halfheusler(a=6.1094)
    s = ref.supercell([2, 1, 1])
    rng = np.random.default_rng(seed)
    cart = s.cart + rng.normal(0, 1e-4, s.cart.shape)
    lat = s.lattice_array + rng.normal(0, 1e-4, (3, 3))
    noisy = Structure(lat, s.species, cart @ np.linalg.inv(lat))
    assert noisy.fingerprint() == ref.fingerprint()
    assert noisy.matches(ref)


def test_rounding_edge_needs_matches():
    """a = 6.113 puts the primitive length at 4.32253 Å, on a rounding edge: noise can
    flip the hash (documented limitation); matches() still identifies the structures."""
    s = halfheusler(a=6.113)
    other = halfheusler(a=6.1128)          # primitive length 4.32239 -> rounds to 4.322
    assert other.fingerprint() != s.fingerprint()
    assert other.matches(s)
    assert not halfheusler("Ni", "Zr", "Sn").matches(s)


def test_fingerprint_distinct_orderings():
    a = halfheusler("Zr", "Ni", "Sn")       # Ni tetrahedral (known)
    b = halfheusler("Ni", "Zr", "Sn")       # Zr tetrahedral
    c = halfheusler("Zr", "Sn", "Ni")       # Sn tetrahedral
    fps = {a.fingerprint(), b.fingerprint(), c.fingerprint()}
    assert len(fps) == 3
    # (X, Y, Z) and (Z, Y, X) are the same crystal (inversion of the rocksalt pair)
    assert halfheusler("Sn", "Ni", "Zr").fingerprint() == a.fingerprint()


def test_fingerprint_distinct_compositions_and_lattice():
    a = halfheusler()
    assert halfheusler("Hf").fingerprint() != a.fingerprint()
    assert halfheusler(a=ZRNISN_A + 0.01).fingerprint() != a.fingerprint()
    assert halfheusler(a=ZRNISN_A).scaled(0.02).fingerprint() != a.fingerprint()


def test_fingerprint_enantiomorphs_distinct():
    s = triclinic()
    mirror = Structure(s.lattice_array * np.array([1, 1, -1]), s.species, s.frac)
    assert mirror.fingerprint() != s.fingerprint()


def test_canonical_form_payload(zrnisn):
    cf = canonical_form(zrnisn)
    assert cf["cell"][:3] == ["4.323"] * 3 and cf["cell"][3:] == ["60.00"] * 3
    assert [r[0] for r in cf["sites"]] == ["Ni", "Sn", "Zr"]


# ---- derived ------------------------------------------------------------------------------

def test_derived(zrnisn):
    assert zrnisn.formula() == "Zr1 Ni1 Sn1"
    assert zrnisn.reduced_formula() == "ZrNiSn"
    assert zrnisn.spacegroup() == (216, "F-43m")
    assert zrnisn.volume() == pytest.approx(ZRNISN_A ** 3 / 4)
    assert zrnisn.scaled(0.03).volume() == pytest.approx(zrnisn.volume() * 1.03)
    assert conventional_hh().reduced_formula() == "ZrNiSn"
    assert conventional_hh().formula() == "Zr4 Ni4 Sn4"


def test_primitive_from_conventional():
    p = conventional_hh().primitive()
    assert p.nsites == 3
    assert p.volume() == pytest.approx(ZRNISN_A ** 3 / 4)
    assert p.fingerprint() == halfheusler().fingerprint()


def test_primitive_keeps_magnetic_cell():
    # rocksalt-like AFM: two Ni with opposite moments must not merge
    a = 4.17
    s = Structure(np.eye(3) * a, ["Ni"] * 4 + ["O"] * 4,
                  [[0, 0, 0], [0, .5, .5], [.5, 0, .5], [.5, .5, 0],
                   [.5, .5, .5], [.5, 0, 0], [0, .5, 0], [0, 0, .5]],
                  magmoms=[2, 2, -2, -2, 0, 0, 0, 0])
    p = s.primitive()
    assert p.nsites < s.nsites
    assert sorted(set(p.magmoms)) == [-2.0, 0.0, 2.0]
    nm = s.copy(magmoms=[])
    assert nm.primitive().nsites == 2


def test_validation():
    with pytest.raises(ValueError):
        Structure(fcc_lattice(5), ["Zr"], [[0, 0, 0], [0.5, 0.5, 0.5]])
    with pytest.raises(ValueError):
        Structure(fcc_lattice(5), ["Xx"], [[0, 0, 0]])
    with pytest.raises(ValueError):
        Structure(np.zeros((3, 3)), ["Zr"], [[0, 0, 0]])
    with pytest.raises(ValueError):
        Structure(fcc_lattice(5), ["Zr"], [[0, 0, 0]], magmoms=[1, 2])


# ---- conversions --------------------------------------------------------------------------

def test_json_round_trip(zrnisn):
    s = zrnisn.copy(source="crystal.halfheusler", parent_structure_id="st-1",
                    magmoms=[0.0, 0.5, 0.0])
    d = s.to_json()
    assert len(d["lattice"]) == 9
    assert d["sites"][1] == {"species": "Ni", "frac": [0.25, 0.25, 0.25]}
    for k, v in {"formula": "Zr1 Ni1 Sn1", "reduced_formula": "ZrNiSn",
                 "spacegroup_number": 216, "spacegroup_symbol": "F-43m",
                 "prototype": "half-heusler", "source": "crystal.halfheusler",
                 "parent_structure_id": "st-1", "magmoms": [0.0, 0.5, 0.0]}.items():
        assert d[k] == v, k
    assert d["fingerprint"] == s.fingerprint()
    back = Structure.from_json(json.loads(json.dumps(d)))
    assert back == s
    # empty fields omitted
    bare = Structure(zrnisn.lattice, zrnisn.species, zrnisn.frac).to_json()
    assert not {"prototype", "source", "parent_structure_id", "magmoms"} & set(bare)


def test_from_json_variants(zrnisn):
    d = zrnisn.to_json()
    d["lattice"] = zrnisn.lattice              # 3x3 accepted
    d["parentStructureId"] = "st-9"            # proto3 JSON camelCase
    d["fingerprint"] = "fp1:bogus"             # derived fields ignored
    s = Structure.from_json(d)
    assert s.parent_structure_id == "st-9" and s.fingerprint() == zrnisn.fingerprint()
    pmg = Structure.from_json(zrnisn.to_pymatgen().as_dict())
    assert pmg.fingerprint() == zrnisn.fingerprint()


def test_proto_round_trip(zrnisn):
    from fireforge.tdr.v1 import record_pb2
    msg = zrnisn.to_proto()
    assert isinstance(msg, record_pb2.Structure)
    assert msg.reduced_formula == "ZrNiSn" and msg.spacegroup_number == 216
    assert msg.fingerprint == zrnisn.fingerprint() and len(msg.lattice) == 9
    wire = record_pb2.Structure.FromString(msg.SerializeToString())
    assert Structure.from_proto(wire) == zrnisn


def test_pymatgen_round_trip(zrnisn):
    s = zrnisn.copy(magmoms=[0.0, 1.0, 0.0])
    p = s.to_pymatgen()
    assert p.composition.reduced_formula == "ZrNiSn"
    assert p.get_space_group_info()[1] == 216
    back = Structure.from_pymatgen(p, prototype="half-heusler")
    assert back == s


def test_ase_round_trip(zrnisn):
    s = zrnisn.copy(magmoms=[0.0, 1.0, 0.0])
    atoms = s.to_ase()
    assert atoms.get_chemical_formula() == "NiSnZr"
    assert np.allclose(atoms.get_initial_magnetic_moments(), [0, 1, 0])
    back = Structure.from_ase(atoms, prototype="half-heusler")
    assert np.allclose(back.lattice, s.lattice) and np.allclose(back.frac, s.frac)
    assert back.species == s.species and back.magmoms == s.magmoms
    assert Structure.from_ase(zrnisn.to_ase()).magmoms == []


def test_save_load(tmp_path, zrnisn):
    p = tmp_path / "s.json"
    save_structure(zrnisn, p)
    assert load_structure(p) == zrnisn
    q = tmp_path / "wrapped.json"
    q.write_text(json.dumps({"structure": zrnisn.to_json()}))
    assert load_structure(q) == zrnisn
    cif = tmp_path / "s.cif"
    cif.write_text(zrnisn.to_pymatgen().to(fmt="cif"))
    assert load_structure(cif).fingerprint() == zrnisn.fingerprint()
