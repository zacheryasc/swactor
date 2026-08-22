use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use syn::visit::Visit;
use syn::{Attribute, Meta};

const SOURCE_ROOTS: &[&str] = &["src", "crates", "apps", "xtask", "tools", "tests"];
const FORBIDDEN_ATTRIBUTES: &[&str] = &["allow", "expect"];

#[derive(Default)]
struct SuppressionVisitor {
    found: bool,
}

impl<'ast> Visit<'ast> for SuppressionVisitor {
    fn visit_attribute(&mut self, attribute: &'ast Attribute) {
        if FORBIDDEN_ATTRIBUTES
            .iter()
            .any(|name| attribute.path().is_ident(name))
            || matches!(
                &attribute.meta,
                Meta::List(list)
                    if list.path.is_ident("cfg_attr") && tokens_contain_suppression(&list.tokens)
            )
        {
            self.found = true;
        }
        syn::visit::visit_attribute(self, attribute);
    }
}

#[test]
fn workspace_sources_do_not_suppress_lints() {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut rust_sources = Vec::new();
    for root in SOURCE_ROOTS {
        collect_rust_sources(&workspace.join(root), &mut rust_sources);
    }
    rust_sources.sort();

    let mut violations = Vec::new();
    for source in rust_sources {
        let text = fs::read_to_string(&source)
            .unwrap_or_else(|error| panic!("read {}: {error}", source.display()));
        let syntax = syn::parse_file(&text)
            .unwrap_or_else(|error| panic!("parse {}: {error}", source.display()));
        let mut visitor = SuppressionVisitor::default();
        visitor.visit_file(&syntax);
        if visitor.found {
            violations.push(
                source
                    .strip_prefix(&workspace)
                    .unwrap_or(&source)
                    .display()
                    .to_string(),
            );
        }
    }

    assert!(
        violations.is_empty(),
        "lint suppression attributes are forbidden; fix the warning instead:\n{}",
        violations.join("\n")
    );
}

#[test]
fn suppression_visitor_detects_direct_and_conditional_attributes() {
    for source in [
        "#[allow(dead_code)] fn hidden() {}",
        "#![expect(unused_imports)]",
        "#[cfg_attr(test, allow(clippy::too_many_arguments))] fn hidden() {}",
        "#![cfg_attr(feature = \"strict\", expect(dead_code))]",
    ] {
        let syntax = syn::parse_file(source).expect("valid suppression probe");
        let mut visitor = SuppressionVisitor::default();
        visitor.visit_file(&syntax);
        assert!(visitor.found, "suppression escaped detection: {source}");
    }

    let syntax = syn::parse_file("#[derive(Clone)] struct Clean;").unwrap();
    let mut visitor = SuppressionVisitor::default();
    visitor.visit_file(&syntax);
    assert!(!visitor.found);
}

#[test]
fn source_tree_test_modules_are_wired() {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut test_directories = Vec::new();
    for root in SOURCE_ROOTS {
        collect_source_test_directories(&workspace.join(root), &mut test_directories);
    }
    test_directories.sort();

    let mut unwired = Vec::new();
    for directory in test_directories {
        let module_path = directory.join("mod.rs");
        let module_source = fs::read_to_string(&module_path)
            .unwrap_or_else(|error| panic!("read {}: {error}", module_path.display()));
        let module = syn::parse_file(&module_source)
            .unwrap_or_else(|error| panic!("parse {}: {error}", module_path.display()));
        let declared = module
            .items
            .iter()
            .filter_map(|item| match item {
                syn::Item::Mod(item) => Some(item.ident.to_string()),
                _ => None,
            })
            .collect::<BTreeSet<_>>();

        for entry in fs::read_dir(&directory)
            .unwrap_or_else(|error| panic!("read directory {}: {error}", directory.display()))
        {
            let path = entry.expect("read test-module entry").path();
            if path.extension().is_some_and(|extension| extension == "rs")
                && path.file_stem().is_some_and(|stem| stem != "mod")
            {
                let module_name = path
                    .file_stem()
                    .expect("test module stem")
                    .to_string_lossy()
                    .to_string();
                if !declared.contains(&module_name) {
                    unwired.push(
                        path.strip_prefix(&workspace)
                            .unwrap_or(&path)
                            .display()
                            .to_string(),
                    );
                }
            }
        }
    }

    assert!(
        unwired.is_empty(),
        "source-tree test modules must be declared by their adjacent mod.rs:\n{}",
        unwired.join("\n")
    );
}

fn collect_source_test_directories(directory: &Path, directories: &mut Vec<PathBuf>) {
    if !directory.exists() {
        return;
    }
    if directory.file_name().is_some_and(|name| name == "tests")
        && directory
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|name| name == "src")
    {
        directories.push(directory.to_path_buf());
        return;
    }
    let entries = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("read directory {}: {error}", directory.display()));
    for entry in entries {
        let path = entry.expect("read source-tree entry").path();
        if path.is_dir() {
            collect_source_test_directories(&path, directories);
        }
    }
}

fn collect_rust_sources(directory: &Path, sources: &mut Vec<PathBuf>) {
    if !directory.exists() {
        return;
    }
    let entries = fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("read directory {}: {error}", directory.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|error| panic!("read directory entry: {error}"));
        let path = entry.path();
        if path.is_dir() {
            collect_rust_sources(&path, sources);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            sources.push(path);
        }
    }
}

fn tokens_contain_suppression(tokens: &proc_macro2::TokenStream) -> bool {
    let mut token_trees = tokens.clone().into_iter().peekable();
    while let Some(token) = token_trees.next() {
        match token {
            proc_macro2::TokenTree::Ident(identifier)
                if FORBIDDEN_ATTRIBUTES.iter().any(|name| identifier == *name)
                    && matches!(
                        token_trees.peek(),
                        Some(proc_macro2::TokenTree::Group(group))
                            if group.delimiter() == proc_macro2::Delimiter::Parenthesis
                    ) =>
            {
                return true;
            }
            proc_macro2::TokenTree::Group(group) if tokens_contain_suppression(&group.stream()) => {
                return true;
            }
            _ => {}
        }
    }
    false
}
