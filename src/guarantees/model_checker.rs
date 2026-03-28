//! Minimal exhaustive DFS model checker.
//!
//! Provides the same core API surface as `stateright` — [`Model`] trait,
//! [`Property`] (always/sometimes), and a [`Checker`] with DFS exploration —
//! without requiring an external crate dependency.
//!
//! This keeps Cargo.lock unchanged while still providing genuine exhaustive
//! state-space exploration for the runtime guarantee proofs.

use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

// ── Model trait ──────────────────────────────────────────────────────────────

/// A finite-state model suitable for exhaustive exploration.
pub trait Model: Sized {
    type State: Clone + Debug + Hash + Eq;
    type Action: Clone + Debug + Hash + Eq;

    /// Initial states to begin exploration from.
    fn init_states(&self) -> Vec<Self::State>;

    /// Enumerate all enabled actions in `state`, pushing them into `actions`.
    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>);

    /// Compute the successor state after applying `action`. Return `None` if
    /// the action is a no-op (state unchanged).
    fn next_state(&self, state: &Self::State, action: Self::Action) -> Option<Self::State>;

    /// Properties to verify across all reachable states.
    fn properties(&self) -> Vec<Property<Self>>;

    /// Create a checker for this model.
    fn checker(&self) -> Checker<'_, Self> {
        Checker { model: self }
    }
}

// ── Property ─────────────────────────────────────────────────────────────────

/// The kind of temporal property.
enum PropertyKind {
    /// Must hold in every reachable state.
    Always,
    /// Must hold in at least one reachable state (liveness canary).
    Sometimes,
}

/// A named property checked during model exploration.
pub struct Property<M: Model> {
    name: String,
    kind: PropertyKind,
    checker_fn: Box<dyn Fn(&M, &M::State) -> bool>,
}

impl<M: Model> Property<M> {
    /// Safety invariant: must hold in every reachable state.
    pub fn always(name: &str, f: impl Fn(&M, &M::State) -> bool + 'static) -> Self {
        Self {
            name: name.to_string(),
            kind: PropertyKind::Always,
            checker_fn: Box::new(f),
        }
    }

    /// Liveness canary: must hold in at least one reachable state.
    pub fn sometimes(name: &str, f: impl Fn(&M, &M::State) -> bool + 'static) -> Self {
        Self {
            name: name.to_string(),
            kind: PropertyKind::Sometimes,
            checker_fn: Box::new(f),
        }
    }
}

// ── Checker & DFS ────────────────────────────────────────────────────────────

/// Builder that holds a reference to the model.
pub struct Checker<'a, M: Model> {
    model: &'a M,
}

impl<'a, M: Model> Checker<'a, M> {
    /// Run exhaustive DFS. Named `spawn_dfs` for API compatibility, but runs
    /// synchronously (no threads needed for our bounded models).
    pub fn spawn_dfs(self) -> DfsHandle<M> {
        let properties = self.model.properties();
        let mut visited: HashSet<M::State> = HashSet::new();
        let mut stack: Vec<(M::State, usize)> = Vec::new(); // (state, depth)
        let mut max_depth: usize = 0;
        let mut actions_buf: Vec<M::Action> = Vec::new();

        // Track property results
        let mut always_violated: Vec<Option<String>> = properties
            .iter()
            .map(|_| None)
            .collect();
        let mut sometimes_satisfied: Vec<bool> = properties
            .iter()
            .map(|_| false)
            .collect();

        // Seed with init states
        for s in self.model.init_states() {
            if visited.insert(s.clone()) {
                stack.push((s, 0));
            }
        }

        while let Some((state, depth)) = stack.pop() {
            if depth > max_depth {
                max_depth = depth;
            }

            // Check all properties against this state
            for (i, prop) in properties.iter().enumerate() {
                let holds = (prop.checker_fn)(self.model, &state);
                match prop.kind {
                    PropertyKind::Always => {
                        if !holds && always_violated[i].is_none() {
                            always_violated[i] = Some(format!(
                                "ALWAYS property {:?} violated in state: {:?}",
                                prop.name, state
                            ));
                        }
                    }
                    PropertyKind::Sometimes => {
                        if holds {
                            sometimes_satisfied[i] = true;
                        }
                    }
                }
            }

            // Expand successors
            actions_buf.clear();
            self.model.actions(&state, &mut actions_buf);

            for action in actions_buf.drain(..) {
                if let Some(next) = self.model.next_state(&state, action) {
                    if visited.insert(next.clone()) {
                        stack.push((next, depth + 1));
                    }
                }
            }
        }

        // Build failures list
        let mut failures = Vec::new();
        for (i, prop) in properties.iter().enumerate() {
            match prop.kind {
                PropertyKind::Always => {
                    if let Some(msg) = &always_violated[i] {
                        failures.push(msg.clone());
                    }
                }
                PropertyKind::Sometimes => {
                    if !sometimes_satisfied[i] {
                        failures.push(format!(
                            "SOMETIMES property {:?} was never satisfied across {} states",
                            prop.name,
                            visited.len()
                        ));
                    }
                }
            }
        }

        DfsHandle {
            result: CheckResult {
                unique_states: visited.len(),
                max_depth,
                failures,
            },
            _phantom: PhantomData,
        }
    }
}

/// Handle returned by `spawn_dfs`. Call `.join()` to get the result.
pub struct DfsHandle<M: Model> {
    result: CheckResult,
    _phantom: PhantomData<M>,
}

// Suppress unused type parameter warning
impl<M: Model> DfsHandle<M> {
    /// Consume the handle and return the exploration result.
    pub fn join(self) -> CheckResult {
        self.result
    }
}

/// Result of an exhaustive DFS exploration.
pub struct CheckResult {
    unique_states: usize,
    max_depth: usize,
    failures: Vec<String>,
}

impl CheckResult {
    /// Number of unique states explored.
    pub fn unique_state_count(&self) -> usize {
        self.unique_states
    }

    /// Maximum DFS depth reached.
    pub fn max_depth(&self) -> usize {
        self.max_depth
    }

    /// Panic if any property was violated.
    pub fn assert_properties(&self) {
        if !self.failures.is_empty() {
            let msg = self.failures.join("\n");
            panic!("Property violations:\n{msg}");
        }
    }
}
