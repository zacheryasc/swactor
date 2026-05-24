//! Deterministic PRNG and substream derivation (SIM_SPEC §7.2).
//!
//! All randomness derives from one root seeded by the scenario's `seed`
//! field. Substreams are keyed by a hashed tuple — `("link", from, to)`,
//! `("host", id, label)`, `("mutation", index)` — using a fixed-key
//! SipHash-2-4. The hash function and key are constants of this crate;
//! changing either is a deliberate spec amendment.
//!
//! The substream output is xoshiro256** — small, fast, deterministic,
//! and architecture-agnostic. It is used for u32, u64, and bounded-int
//! draws. Floats are never generated; §7.4 forbids them in decision
//! paths.

use std::hash::Hasher;

use siphasher::sip::SipHasher24;

/// The SipHash key used for every substream derivation. Constant. The
/// pair of 64-bit halves is documented so a port of the simulator to
/// another language can reproduce the same hash output.
pub const SIPHASH_KEY_LO: u64 = 0x0123_4567_89ab_cdef;
pub const SIPHASH_KEY_HI: u64 = 0xfedc_ba98_7654_3210;

/// A xoshiro256** PRNG state. Small (32 bytes), pure, no allocations.
#[derive(Debug, Clone, Copy)]
pub struct SubstreamRng {
    state: [u64; 4],
}

impl SubstreamRng {
    /// Seed a substream from the scenario seed and a structured key.
    /// The same seed and key always produce the same stream.
    pub fn derive(scenario_seed: u64, key: &SubstreamKey) -> Self {
        let mut h = SipHasher24::new_with_keys(SIPHASH_KEY_LO, SIPHASH_KEY_HI);
        h.write_u64(scenario_seed);
        key.hash_into(&mut h);
        let lo = h.finish();
        // Generate a second word so the xoshiro state has 128 bits of
        // independent material; the same hash with one byte of suffix.
        let mut h2 = SipHasher24::new_with_keys(SIPHASH_KEY_LO, SIPHASH_KEY_HI);
        h2.write_u64(scenario_seed);
        key.hash_into(&mut h2);
        h2.write_u8(0x01);
        let hi = h2.finish();
        let mut h3 = SipHasher24::new_with_keys(SIPHASH_KEY_LO, SIPHASH_KEY_HI);
        h3.write_u64(scenario_seed);
        key.hash_into(&mut h3);
        h3.write_u8(0x02);
        let hi2 = h3.finish();
        let mut h4 = SipHasher24::new_with_keys(SIPHASH_KEY_LO, SIPHASH_KEY_HI);
        h4.write_u64(scenario_seed);
        key.hash_into(&mut h4);
        h4.write_u8(0x03);
        let hi3 = h4.finish();
        let mut state = [lo, hi, hi2, hi3];
        // Avoid the all-zero state xoshiro forbids.
        if state == [0; 4] {
            state[0] = 1;
        }
        Self { state }
    }

    /// One xoshiro256** step. Returns a u64 of uniformly-distributed
    /// bits. The constants are the canonical xoshiro256** parameters.
    pub fn next_u64(&mut self) -> u64 {
        let result = self.state[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = self.state[1] << 17;
        self.state[2] ^= self.state[0];
        self.state[3] ^= self.state[1];
        self.state[1] ^= self.state[2];
        self.state[0] ^= self.state[3];
        self.state[2] ^= t;
        self.state[3] = self.state[3].rotate_left(45);
        result
    }

    pub fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    /// Returns a uniform integer in `[0, bound)`. Uses the rejection
    /// method to stay unbiased without floats.
    pub fn gen_range_u32(&mut self, bound: u32) -> u32 {
        assert!(bound > 0, "gen_range_u32 bound must be > 0");
        // Lemire's nearly-divisionless method, integer-only.
        let mut x = self.next_u32() as u64;
        let mut m = x.wrapping_mul(bound as u64);
        let mut l = m as u32;
        if l < bound {
            let t = bound.wrapping_neg() % bound;
            while l < t {
                x = self.next_u32() as u64;
                m = x.wrapping_mul(bound as u64);
                l = m as u32;
            }
        }
        (m >> 32) as u32
    }
}

/// One of the §7.2 substream keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubstreamKey {
    /// Per-edge substream: `("link", from_id, to_id)`.
    Link { from: String, to: String },
    /// Per-host substream: `("host", host_id, label)`. The label
    /// distinguishes uses (e.g. "tick_offset", "swim_internal").
    Host { host_id: String, label: &'static str },
    /// Per-mutation substream: `("mutation", index)`.
    Mutation { index: u64 },
}

impl SubstreamKey {
    fn hash_into<H: Hasher>(&self, h: &mut H) {
        match self {
            SubstreamKey::Link { from, to } => {
                h.write(b"link");
                h.write_u8(0);
                h.write(from.as_bytes());
                h.write_u8(0);
                h.write(to.as_bytes());
            }
            SubstreamKey::Host { host_id, label } => {
                h.write(b"host");
                h.write_u8(0);
                h.write(host_id.as_bytes());
                h.write_u8(0);
                h.write(label.as_bytes());
            }
            SubstreamKey::Mutation { index } => {
                h.write(b"mutation");
                h.write_u8(0);
                h.write_u64(*index);
            }
        }
    }
}

/// Precomputed integer Gaussian table (SIM_SPEC §7.4). 256 samples of
/// the inverse standard-normal CDF on the half-grid
/// `((i + 0.5) / 256)`, scaled by 1024 and rounded to the nearest
/// signed integer. Symmetric around zero by construction (the i-th and
/// (255-i)-th entries sum to zero up to rounding). To draw an integer
/// jitter sample for a given stddev, take a uniform draw `i` in
/// `[0, 256)`, look up `GAUSSIAN_TABLE_1024[i]`, then compute
/// `(sample * stddev + 512) >> 10` for the scaled value.
///
/// The table is checked in literally rather than generated at build
/// time so a third-party reviewer can verify the values byte-for-byte
/// against an independent inverse-normal-CDF implementation.
pub const GAUSSIAN_TABLE_1024: [i32; 256] = [
    -2955, -2581, -2391, -2260, -2157, -2073, -2000, -1937, -1880, -1828, -1781, -1737, -1696, -1658, -1622, -1587,
    -1555, -1524, -1494, -1466, -1438, -1412, -1386, -1362, -1338, -1315, -1292, -1270, -1249, -1228, -1208, -1188,
    -1168, -1149, -1131, -1112, -1094, -1077, -1060, -1043, -1026, -1009, -993, -977, -962, -946, -931, -916,
    -901, -886, -872, -858, -843, -829, -816, -802, -788, -775, -762, -748, -735, -722, -710, -697,
    -684, -672, -660, -647, -635, -623, -611, -599, -587, -575, -564, -552, -540, -529, -518, -506,
    -495, -484, -472, -461, -450, -439, -428, -417, -406, -396, -385, -374, -363, -353, -342, -332,
    -321, -311, -300, -290, -279, -269, -258, -248, -238, -227, -217, -207, -197, -187, -176, -166,
    -156, -146, -136, -126, -116, -105, -95, -85, -75, -65, -55, -45, -35, -25, -15, -5,
    5, 15, 25, 35, 45, 55, 65, 75, 85, 95, 105, 116, 126, 136, 146, 156,
    166, 176, 187, 197, 207, 217, 227, 238, 248, 258, 269, 279, 290, 300, 311, 321,
    332, 342, 353, 363, 374, 385, 396, 406, 417, 428, 439, 450, 461, 472, 484, 495,
    506, 518, 529, 540, 552, 564, 575, 587, 599, 611, 623, 635, 647, 660, 672, 684,
    697, 710, 722, 735, 748, 762, 775, 788, 802, 816, 829, 843, 858, 872, 886, 901,
    916, 931, 946, 962, 977, 993, 1009, 1026, 1043, 1060, 1077, 1094, 1112, 1131, 1149, 1168,
    1188, 1208, 1228, 1249, 1270, 1292, 1315, 1338, 1362, 1386, 1412, 1438, 1466, 1494, 1524, 1555,
    1587, 1622, 1658, 1696, 1737, 1781, 1828, 1880, 1937, 2000, 2073, 2157, 2260, 2391, 2581, 2955,
];

/// Draw one integer-Gaussian jitter sample scaled to `stddev_ns`. The
/// sample comes from [`GAUSSIAN_TABLE_1024`]; we multiply by `stddev`,
/// add 512 (half the 1024 scale), and shift right by 10. Result is in
/// integer nanoseconds, can be negative.
pub fn jitter_sample(rng: &mut SubstreamRng, stddev_ns: u64) -> i64 {
    if stddev_ns == 0 {
        return 0;
    }
    let idx = rng.gen_range_u32(GAUSSIAN_TABLE_1024.len() as u32) as usize;
    let raw = GAUSSIAN_TABLE_1024[idx] as i64;
    // (raw * stddev + 512) >> 10, taking sign of raw.
    let stddev = stddev_ns as i64;
    let scaled = raw.saturating_mul(stddev);
    if scaled >= 0 {
        (scaled + 512) >> 10
    } else {
        -((-scaled + 512) >> 10)
    }
}
