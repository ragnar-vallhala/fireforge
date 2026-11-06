"""Slack model of the lattice thermal conductivity (docs/design/phase8.md §4).

    κ_l(T) = A(γ) · M̄ · θ_a³ · δ · n^{1/3} / (γ² · T)

with (SI-consistent form, Julian's prefactor):

    A(γ) = 0.849 · 3 · 4^{1/3} / (20 π³ (1 − 0.514/γ + 0.228/γ²)) · (k_B/ħ)³

    M̄    mean atomic mass                     kg
    θ_a   acoustic Debye temperature           K    (θ_a = θ_D · n^{−1/3})
    δ     cube root of the volume per atom     m
    n     atoms in the primitive cell          -
    γ     Grüneisen parameter (acoustic)       -
    T     temperature                          K
    κ_l                                        W m⁻¹ K⁻¹

In Slack's practical units (M̄ in amu, δ in Å) the prefactor is
A = 2.43·10⁻⁸ / (1 − 0.514/γ + 0.228/γ²) **W cm⁻¹ K⁻¹** (Slack's original unit), i.e.
2.43·10⁻⁶ / (…) W m⁻¹ K⁻¹. The often quoted "2.43e-8" therefore gives κ in W/(cm·K);
using it with W/(m·K) is wrong by a factor 100. `julian_a_practical` returns the W/(m·K)
value; the tests check it against the SI expression.

References: C. L. Julian, Phys. Rev. 137, A128 (1965) (fit of the prefactor's γ
dependence); G. A. Slack, Solid State Physics 34, 1 (1979); D. T. Morelli and
G. A. Slack, in High Thermal Conductivity Materials (Springer, 2006), ch. 2,
doi:10.1007/0-387-25100-6_2 (κ = A M̄ θ_a³ δ n^{1/3}/(γ² T)); the SI form with
0.849·3·4^{1/3}/(20π³) is eq. (7) of Z. Ma et al., arXiv:1811.09968.
"""
from __future__ import annotations

import math
from typing import Iterable

KB = 1.380649e-23          # J/K
HBAR = 1.054571817e-34     # J s
AMU = 1.66053906660e-27    # kg
ANGSTROM = 1e-10           # m


def julian_denominator(gamma: float) -> float:
    """1 − 0.514/γ + 0.228/γ² (Julian 1965)."""
    if gamma <= 0:
        raise ValueError(f"Slack model needs γ > 0, got {gamma}")
    return 1.0 - 0.514 / gamma + 0.228 / gamma ** 2


def julian_a_si(gamma: float) -> float:
    """A(γ) in SI: W m⁻¹ K⁻¹ per (kg · m · K³ / K) = κ·γ²·T / (M̄ θ_a³ δ n^{1/3})."""
    pref = 0.849 * 3.0 * 4.0 ** (1.0 / 3.0) / (20.0 * math.pi ** 3 * julian_denominator(gamma))
    return pref * (KB / HBAR) ** 3


def julian_a_practical(gamma: float) -> float:
    """A(γ) in W m⁻¹ K⁻¹ for M̄ in amu and δ in Å (≈ 2.43e-6 / (1 − 0.514/γ + 0.228/γ²))."""
    return julian_a_si(gamma) * AMU * ANGSTROM


def slack_kappa(t_k: float | Iterable[float], *, mean_mass_amu: float, theta_a_k: float,
                delta_a: float, n_atoms: int, gamma: float):
    """κ_l in W/(m·K) at temperature(s) t_k (K). Inputs: mean atomic mass (amu), acoustic
    Debye temperature θ_a (K), δ = (V/atom)^{1/3} (Å), atoms per primitive cell, γ."""
    if mean_mass_amu <= 0 or theta_a_k <= 0 or delta_a <= 0 or n_atoms < 1:
        raise ValueError("Slack model needs positive mass, θ_a, δ and n ≥ 1")
    coeff = (julian_a_si(gamma) * mean_mass_amu * AMU * theta_a_k ** 3 * delta_a * ANGSTROM
             * n_atoms ** (1.0 / 3.0) / gamma ** 2)          # W/m (= κ·T)
    if isinstance(t_k, (int, float)):
        return coeff / float(t_k)
    return [coeff / float(t) for t in t_k]


def slack_kappa_times_t(**kw) -> float:
    """κ_l·T (W/m), temperature independent in the Slack model."""
    return slack_kappa(1.0, **kw)
