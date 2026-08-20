use rand::{RngExt, SeedableRng};
use rand_distr::{Distribution, Poisson};
use rand_pcg::Pcg64;

/// Default maximum gap-sampling fraction.
/// For sampling fractions <= this value, the gap sampling optimization will be applied.
/// Above this value, it is assumed that "traditional" Bernoulli sampling is faster. The
/// optimal value for this will depend on the RNG.  More expensive RNGs will tend to make
/// the optimal value higher. The most reliable way to determine this value for a new RNG
/// is to experiment. When tuning for a new RNG, expect a value of 0.5 to be close in
/// most cases, as an initial guess.
// TODO: tune for PCG64, performance is similar and around same order of magnitude
// of XORShift so shouldn't be too far off
pub const DEFAULT_MAX_GAP_SAMPLING_FRACTION: f64 = 0.4;

/// Default epsilon for floating point numbers sampled from the RNG.
/// The gap-sampling compute logic requires taking log(x), where x is sampled from an RNG.
/// To guard against errors from taking log(0), a positive epsilon lower bound is applied.
/// A good value for this parameter is at or near the minimum positive floating
/// point value returned by for the RNG being used.
// TODO: this is a straight port, it may not apply exactly to pcg64 rng but should be mostly fine;
// double check; an XORShift generator is used by default
pub const RNG_EPSILON: f64 = 5e-11;

/// Sampling fraction arguments may be results of computation, and subject to floating
/// point jitter.  I check the arguments with this epsilon slop factor to prevent spurious
/// warnings for cases such as summing some numbers to get a sampling fraction of 1.000000001
pub const ROUNDING_EPSILON: f64 = 1e-6;

pub type RSamplerFunc<T> =
    Box<dyn Fn(Box<dyn Iterator<Item = T>>) -> Box<dyn Iterator<Item = T>> + 'static>;

pub trait RandomSampler<T>: Send + Sync {
    /// Returns a function which returns random samples,
    /// the sampler is thread-safe as the RNG is seeded with random seeds per thread.
    fn get_sampler(&self, seed: Option<u64>) -> RSamplerFunc<T>;
}

pub fn get_default_rng() -> Pcg64 {
    Pcg64::new(
        0xcafe_f00d_d15e_a5e5,
        0x0a02_bdbf_7bb3_c0a7_ac28_fa16_a64a_bf96,
    )
}

pub fn rng_from_seed(seed: u64) -> Pcg64 {
    Pcg64::seed_from_u64(seed)
}

/// Get a new rng with random thread local random seed
fn random_rng() -> Pcg64 {
    Pcg64::seed_from_u64(rand::random::<u64>())
}

#[derive(Clone, Copy)]
pub struct PoissonSampler {
    fraction: f64,
    use_gap_sampling_if_possible: bool,
    prob: f64,
}

impl PoissonSampler {
    pub fn new(fraction: f64, use_gap_sampling_if_possible: bool) -> PoissonSampler {
        let prob = if fraction > 0.0 { fraction } else { 1.0 };

        PoissonSampler {
            fraction,
            use_gap_sampling_if_possible,
            prob,
        }
    }
}

impl<T: Clone + 'static> RandomSampler<T> for PoissonSampler {
    fn get_sampler(&self, seed: Option<u64>) -> RSamplerFunc<T> {
        let fraction = self.fraction;
        let use_gap_sampling_if_possible = self.use_gap_sampling_if_possible;
        let prob = self.prob;

        Box::new(
            move |items: Box<dyn Iterator<Item = T>>| -> Box<dyn Iterator<Item = T>> {
                if fraction <= 0.0 {
                    Box::new(std::iter::empty())
                } else {
                    let use_gap_sampling = use_gap_sampling_if_possible
                        && fraction <= DEFAULT_MAX_GAP_SAMPLING_FRACTION;

                    let mut gap_sampling = if use_gap_sampling {
                        // Initialize here and move to avoid constructing a new one each iteration
                        Some(GapSamplingReplacement::new(fraction, RNG_EPSILON))
                    } else {
                        None
                    };
                    let dist = Poisson::new(prob).unwrap();

                    let mut rng: Pcg64 = match seed {
                        Some(s) => rng_from_seed(s),
                        None => random_rng(),
                    };

                    Box::new(items.flat_map(move |item| {
                        let count = if use_gap_sampling {
                            gap_sampling.as_mut().unwrap().sample()
                        } else {
                            dist.sample(&mut rng) as u64
                        };
                        if count != 0 {
                            vec![item; count as usize]
                        } else {
                            vec![]
                        }
                    }))
                }
            },
        )
    }
}

#[derive(Clone, Copy)]
pub struct BernoulliSampler {
    fraction: f64,
}

impl BernoulliSampler {
    pub fn new(fraction: f64) -> BernoulliSampler {
        assert!(((0.0 - ROUNDING_EPSILON)..=(1.0 + ROUNDING_EPSILON)).contains(&fraction));
        BernoulliSampler { fraction }
    }

    fn sample(&self, gap_sampling: Option<&mut GapSamplingReplacement>, rng: &mut Pcg64) -> u64 {
        match self.fraction {
            v if v <= 0.0 => 0,
            v if v >= 1.0 => 1,
            v if v <= DEFAULT_MAX_GAP_SAMPLING_FRACTION => gap_sampling.unwrap().sample(),
            v if rng.random::<f64>() <= v => 1,
            _ => 0,
        }
    }
}

impl<T: 'static> RandomSampler<T> for BernoulliSampler {
    fn get_sampler(&self, seed: Option<u64>) -> RSamplerFunc<T> {
        let fraction = self.fraction;
        let mine = *self;

        Box::new(
            move |items: Box<dyn Iterator<Item = T>>| -> Box<dyn Iterator<Item = T>> {
                let mut gap_sampling = if fraction > 0.0 && fraction < 1.0 {
                    Some(GapSamplingReplacement::new(fraction, RNG_EPSILON))
                } else {
                    None
                };

                let mut rng: Pcg64 = match seed {
                    Some(s) => rng_from_seed(s),
                    None => random_rng(),
                };

                Box::new(items.filter(move |_| mine.sample(gap_sampling.as_mut(), &mut rng) > 0))
            },
        )
    }
}

struct GapSamplingReplacement {
    fraction: f64,
    epsilon: f64,
    q: f64,
    rng: rand_pcg::Pcg64,
    count_for_dropping: u64,
}

impl GapSamplingReplacement {
    fn new(fraction: f64, epsilon: f64) -> GapSamplingReplacement {
        assert!(fraction > 0.0 && fraction < 1.0);
        assert!(epsilon > 0.0);

        let mut sampler = GapSamplingReplacement {
            q: (-fraction).exp(),
            fraction,
            epsilon,
            rng: random_rng(),
            count_for_dropping: 0,
        };
        // Advance to first sample as part of object construction.
        sampler.advance();
        sampler
    }

    fn sample(&mut self) -> u64 {
        if self.count_for_dropping > 0 {
            self.count_for_dropping -= 1;
            0
        } else {
            let r = self.poisson_ge1();
            self.advance();
            r
        }
    }

    /// Sample from Poisson distribution, conditioned such that the sampled value is >= 1.
    /// This is an adaptation from the algorithm for generating
    /// [Poisson distributed random variables](http://en.wikipedia.org/wiki/Poisson_distribution)
    fn poisson_ge1(&mut self) -> u64 {
        // simulate that the standard poisson sampling
        // gave us at least one iteration, for a sample of >= 1
        let mut pp: f64 = self.q + ((1.0 - self.q) * self.rng.random::<f64>());
        let mut r = 1;

        // now continue with standard poisson sampling algorithm
        pp *= self.rng.random::<f64>();
        while pp > self.q {
            r += 1;
            pp *= self.rng.random::<f64>()
        }
        r
    }

    /// Skip elements with replication factor zero (i.e. elements that won't be sampled).
    /// Samples 'k' from geometric distribution P(k) = (1-q)(q)^k, where q = e^(-f), that is
    /// q is the probability of Poisson(0; f)
    fn advance(&mut self) {
        let u = self.epsilon.max(self.rng.random::<f64>());
        self.count_for_dropping = (u.log(std::f64::consts::E) / (-self.fraction)) as u64;
    }
}
