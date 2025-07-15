use std::sync::OnceLock;

use parking_lot::Mutex;
use sedona_common::ExecutionMode;
use sedona_expr::statistics::GeoStatistics;

/// Select the optimal execution mode for the refinement phase of spatial join
/// using the build-side and partial probe-side statistics.
pub(crate) struct ExecModeSelector<F: Fn(&GeoStatistics, &GeoStatistics) -> ExecutionMode> {
    /// The build-side statistics.
    build_stats: GeoStatistics,
    /// The partial probe-side statistics.
    probe_stats: Mutex<GeoStatistics>,
    /// The minimum number of probe-side geometry to analyze before selecting the execution mode.
    min_required_count: usize,
    /// The optimal execution mode selected.
    optimal_mode: OnceLock<ExecutionMode>,
    /// The function to select the optimal execution mode.
    func: F,
}

impl<F: Fn(&GeoStatistics, &GeoStatistics) -> ExecutionMode> ExecModeSelector<F> {
    pub(crate) fn new(build_stats: GeoStatistics, min_required_count: usize, func: F) -> Self {
        Self {
            build_stats,
            probe_stats: Mutex::new(GeoStatistics::empty()),
            min_required_count,
            optimal_mode: OnceLock::new(),
            func,
        }
    }

    pub(crate) fn merge_probe_stats(&self, stats: GeoStatistics) {
        let mut probe_stats = self.probe_stats.lock();
        probe_stats.merge(&stats);
        let analyzed_count = probe_stats.total_geometries().unwrap_or(0) as usize;
        if analyzed_count >= self.min_required_count {
            self.optimal_mode
                .get_or_init(|| self.select_optimal_mode(&probe_stats));
        }
    }

    pub(crate) fn optimal_mode(&self) -> Option<ExecutionMode> {
        self.optimal_mode.get().copied()
    }

    fn select_optimal_mode(&self, probe_stats: &GeoStatistics) -> ExecutionMode {
        (self.func)(&self.build_stats, probe_stats)
    }
}

pub type SelectorFunc = fn(&GeoStatistics, &GeoStatistics) -> ExecutionMode;

pub(crate) fn create_exec_mode_selector(
    build_stats: GeoStatistics,
    exec_mode: ExecutionMode,
    func: SelectorFunc,
) -> Option<ExecModeSelector<SelectorFunc>> {
    if let ExecutionMode::Speculative(n) = exec_mode {
        Some(ExecModeSelector::new(build_stats, n, func))
    } else {
        None
    }
}

/// Get the current execution mode or update it with the optimal mode if it is not set.
/// If optimal mode is not selected yet, return the default execution mode without updating
/// the execution mode.
pub(crate) fn get_or_update_execution_mode(
    exec_mode: &OnceLock<ExecutionMode>,
    exec_mode_selector: &Option<ExecModeSelector<SelectorFunc>>,
    default_exec_mode: ExecutionMode,
) -> ExecutionMode {
    if let Some(mode) = exec_mode.get() {
        *mode
    } else if let Some(selector) = exec_mode_selector {
        if let Some(mode) = selector.optimal_mode() {
            *exec_mode.get_or_init(|| mode)
        } else {
            default_exec_mode
        }
    } else {
        default_exec_mode
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mock_selector_func(_build: &GeoStatistics, _probe: &GeoStatistics) -> ExecutionMode {
        ExecutionMode::PrepareBuild
    }

    fn create_stats_with_geom_count(count: i64) -> GeoStatistics {
        GeoStatistics::empty().with_total_geometries(count)
    }

    #[test]
    fn test_exec_mode_selector_progression() {
        let build_stats = create_stats_with_geom_count(100);
        let selector = ExecModeSelector::new(build_stats, 50, mock_selector_func as SelectorFunc);

        // Initial state: no optimal mode is selected
        assert_eq!(selector.optimal_mode(), None);

        // Ingest first batch (20 < 50 threshold)
        let probe_stats_1 = create_stats_with_geom_count(20);
        selector.merge_probe_stats(probe_stats_1);
        assert_eq!(selector.optimal_mode(), None);

        // Ingest second batch (total: 20 + 25 = 45 < 50 threshold)
        let probe_stats_2 = create_stats_with_geom_count(25);
        selector.merge_probe_stats(probe_stats_2);
        assert_eq!(selector.optimal_mode(), None);

        // Ingest third batch (total: 45 + 10 = 55 >= 50 threshold)
        let probe_stats_3 = create_stats_with_geom_count(10);
        selector.merge_probe_stats(probe_stats_3);
        assert_eq!(selector.optimal_mode(), Some(ExecutionMode::PrepareBuild));

        // Further ingestion should not change the selected mode
        let probe_stats_4 = create_stats_with_geom_count(100);
        selector.merge_probe_stats(probe_stats_4);
        assert_eq!(selector.optimal_mode(), Some(ExecutionMode::PrepareBuild));
    }

    #[test]
    fn test_get_or_update_execution_mode_ready() {
        let exec_mode = OnceLock::new();
        exec_mode.set(ExecutionMode::PrepareProbe).unwrap();
        let selector = None;

        // Case 1: exec_mode is ready
        let result =
            get_or_update_execution_mode(&exec_mode, &selector, ExecutionMode::PrepareNone);
        assert_eq!(result, ExecutionMode::PrepareProbe);
    }

    #[test]
    fn test_get_or_update_execution_mode_neither_ready() {
        let exec_mode = OnceLock::new();
        let selector = None;

        // Case 2: exec_mode is not ready, exec_mode_selector is not ready either
        let result =
            get_or_update_execution_mode(&exec_mode, &selector, ExecutionMode::PrepareNone);
        assert_eq!(result, ExecutionMode::PrepareNone);
        // exec_mode should still be empty
        assert!(exec_mode.get().is_none());
    }

    #[test]
    fn test_get_or_update_execution_mode_selector_ready() {
        let exec_mode = OnceLock::new();
        let build_stats = create_stats_with_geom_count(100);
        let selector = ExecModeSelector::new(build_stats, 10, mock_selector_func as SelectorFunc);

        // Make selector ready by providing enough probe stats
        let probe_stats = create_stats_with_geom_count(20);
        selector.merge_probe_stats(probe_stats);

        // Case 3: exec_mode is not ready, exec_mode_selector has selected an optimal mode
        let result =
            get_or_update_execution_mode(&exec_mode, &Some(selector), ExecutionMode::PrepareNone);
        assert_eq!(result, ExecutionMode::PrepareBuild);
        // exec_mode should now be set
        assert_eq!(exec_mode.get(), Some(&ExecutionMode::PrepareBuild));
    }

    #[test]
    fn test_create_exec_mode_selector() {
        let build_stats = create_stats_with_geom_count(100);

        // Should create selector for Speculative mode
        let selector = create_exec_mode_selector(
            build_stats.clone(),
            ExecutionMode::Speculative(25),
            mock_selector_func as SelectorFunc,
        );
        assert!(selector.is_some());

        // Should return None for non-Speculative modes
        let selector = create_exec_mode_selector(
            build_stats,
            ExecutionMode::PrepareNone,
            mock_selector_func as SelectorFunc,
        );
        assert!(selector.is_none());
    }
}
