//! Combinatorial Purged Cross-Validation з ембарго (M1.2, López de Prado).
//!
//! Класична CV тече в часових рядах: сусідні семпли корельовані, а лейбл
//! семпла i "живе" ще `label_duration` барів після i. Тому:
//! 1) purge — з train вирізаються семпли, чиї лейбли перекриваються з test;
//! 2) embargo — додатковий зазор ПІСЛЯ test-блоку, що виключає leakage через
//!    серійну кореляцію фіч.

use serde::{Deserialize, Serialize};

/// Одна CV-комбінація: індекси train і test у [0, n_samples).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PurgedFold {
    pub train: Vec<usize>,
    pub test: Vec<usize>,
    /// Які групи (блоки) утворюють test у цій комбінації.
    pub test_groups: Vec<usize>,
}

#[derive(Debug, Clone)]
pub struct PurgedCvConfig {
    /// Кількість послідовних блоків, на які ріжеться ряд.
    pub n_groups: usize,
    /// Скільки блоків одночасно йдуть у test (комбінаторно).
    pub n_test_groups: usize,
    /// Тривалість лейбла в барах: лейбл семпла i використовує інформацію
    /// [i, i + label_duration].
    pub label_duration: usize,
    /// Ембарго в барах після test-блоку.
    pub embargo: usize,
}

/// Генерує всі C(n_groups, n_test_groups) комбінації purged-фолдів.
pub fn combinatorial_purged_folds(n_samples: usize, cfg: &PurgedCvConfig) -> Vec<PurgedFold> {
    assert!(cfg.n_groups >= 2, "need at least 2 groups");
    assert!(
        cfg.n_test_groups >= 1 && cfg.n_test_groups < cfg.n_groups,
        "n_test_groups must be in [1, n_groups)"
    );
    if n_samples < cfg.n_groups {
        return Vec::new();
    }

    // Межі блоків: [start, end) послідовно, майже рівні.
    let bounds: Vec<(usize, usize)> = (0..cfg.n_groups)
        .map(|g| {
            let start = g * n_samples / cfg.n_groups;
            let end = (g + 1) * n_samples / cfg.n_groups;
            (start, end)
        })
        .collect();

    let combos = combinations(cfg.n_groups, cfg.n_test_groups);
    let mut folds = Vec::with_capacity(combos.len());

    for combo in combos {
        let mut test: Vec<usize> = Vec::new();
        for &g in &combo {
            test.extend(bounds[g].0..bounds[g].1);
        }

        // train: усе, що (а) не в test, (б) лейбл не перекривається з test
        // (purge), (в) не в embargo-зоні після test-блоку.
        let mut excluded = vec![false; n_samples];
        for &g in &combo {
            let (start, end) = bounds[g];
            // Сам test-блок.
            for e in excluded.iter_mut().take(end).skip(start) {
                *e = true;
            }
            // Purge ПЕРЕД блоком: семпл i з лейблом [i, i+d] заглядає в test,
            // якщо i + d >= start → вирізаємо [start - d, start).
            let purge_from = start.saturating_sub(cfg.label_duration);
            for e in excluded.iter_mut().take(start).skip(purge_from) {
                *e = true;
            }
            // Embargo ПІСЛЯ блоку: [end, end + embargo).
            let embargo_to = (end + cfg.embargo).min(n_samples);
            for e in excluded.iter_mut().take(embargo_to).skip(end) {
                *e = true;
            }
        }

        let train: Vec<usize> = (0..n_samples).filter(|i| !excluded[*i]).collect();
        folds.push(PurgedFold {
            train,
            test,
            test_groups: combo,
        });
    }

    folds
}

fn combinations(n: usize, k: usize) -> Vec<Vec<usize>> {
    let mut out = Vec::new();
    let mut current = Vec::with_capacity(k);
    fn rec(start: usize, n: usize, k: usize, current: &mut Vec<usize>, out: &mut Vec<Vec<usize>>) {
        if current.len() == k {
            out.push(current.clone());
            return;
        }
        for i in start..n {
            current.push(i);
            rec(i + 1, n, k, current, out);
            current.pop();
        }
    }
    rec(0, n, k, &mut current, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(label: usize, embargo: usize) -> PurgedCvConfig {
        PurgedCvConfig {
            n_groups: 5,
            n_test_groups: 1,
            label_duration: label,
            embargo,
        }
    }

    // M1.2: жоден test-семпл не має інформаційного перекриття з train:
    // для будь-якого train-семпла i та test-семпла j:
    //   [i, i+d] ∩ test-блок = ∅  і  i не в embargo-зоні після test.
    #[test]
    fn no_informational_overlap_between_train_and_test() {
        let n = 100;
        let d = 5;
        let embargo = 3;
        let folds = combinatorial_purged_folds(n, &cfg(d, embargo));
        assert!(!folds.is_empty());
        for fold in &folds {
            let test_min = *fold.test.iter().min().unwrap();
            let test_max = *fold.test.iter().max().unwrap();
            for &i in &fold.train {
                // Лейбл train-семпла [i, i+d] не сягає test-блоку.
                let label_end = i + d;
                let overlaps = i <= test_max && label_end >= test_min;
                assert!(
                    !overlaps,
                    "train sample {i} (label to {label_end}) overlaps test [{test_min},{test_max}]"
                );
                // Embargo: train-семпл не одразу після test.
                assert!(
                    !(i > test_max && i < test_max + 1 + embargo),
                    "train sample {i} inside embargo zone after {test_max}"
                );
            }
        }
    }

    // M1.2: між train і test завжди є ембарго-зазор (праворуч від test).
    #[test]
    fn embargo_gap_exists_after_test_block() {
        let n = 100;
        let embargo = 7;
        let folds = combinatorial_purged_folds(n, &cfg(0, embargo));
        for fold in &folds {
            let test_max = *fold.test.iter().max().unwrap();
            if test_max + 1 >= n {
                continue; // test — останній блок, праворуч нічого немає
            }
            let min_train_after: Option<usize> =
                fold.train.iter().copied().filter(|i| *i > test_max).min();
            if let Some(m) = min_train_after {
                assert!(
                    m >= test_max + 1 + embargo,
                    "closest train {m} after test {test_max} violates embargo {embargo}"
                );
            }
        }
    }

    #[test]
    fn combinatorial_generates_all_combinations() {
        let folds = combinatorial_purged_folds(
            100,
            &PurgedCvConfig {
                n_groups: 5,
                n_test_groups: 2,
                label_duration: 0,
                embargo: 0,
            },
        );
        assert_eq!(folds.len(), 10); // C(5,2)
    }

    #[test]
    fn test_blocks_cover_whole_series_across_folds() {
        let n = 50;
        let folds = combinatorial_purged_folds(n, &cfg(0, 0));
        let mut seen = vec![false; n];
        for fold in &folds {
            for &i in &fold.test {
                seen[i] = true;
            }
        }
        assert!(seen.iter().all(|s| *s));
    }
}
