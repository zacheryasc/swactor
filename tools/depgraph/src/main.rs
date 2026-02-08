//! Deterministic dependency graph generator for the swactor crate.
//!
//! Parses all `.rs` source files using `syn`, extracts type definitions,
//! imports, and cross-module dependencies, then outputs `deps.dot` and
//! `deps.html` files.

use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::fs;
use std::path::{Path, PathBuf};

// ─── Data structures ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
enum TypeKind {
    Struct,
    Trait,
    Enum,
}

#[derive(Debug, Clone)]
struct TypeInfo {
    name: String,
    kind: TypeKind,
    fields: Vec<(String, String)>, // (field_name, type_description)
}

#[derive(Debug)]
struct ModuleInfo {
    name: String,
    feature_gate: Option<String>,
    types: Vec<TypeInfo>,
    /// local_name → (source_module, original_name)
    imports: HashMap<String, (String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[allow(dead_code)]
enum EdgeKind {
    Field,
    TraitImpl,
    TraitObject,
}

#[derive(Debug, Clone)]
struct Edge {
    from_module: String,
    from_type: String,
    to_module: String,
    to_type: String,
    kind: EdgeKind,
    label: String,
}

// ─── Module colors ───────────────────────────────────────────────────────────

/// 8-color pastel palette for module clusters.
/// Each entry: (cluster_fill, cluster_border, node_fill)
const PALETTE: &[(&str, &str, &str)] = &[
    ("#e3f2fd", "#1565c0", "#bbdefb"),
    ("#fce4ec", "#c62828", "#ffcdd2"),
    ("#fff3e0", "#e65100", "#ffe0b2"),
    ("#f3e5f5", "#7b1fa2", "#e1bee7"),
    ("#e8f5e9", "#2e7d32", "#c8e6c9"),
    ("#fff9c4", "#f9a825", "#fff59d"),
    ("#e0f7fa", "#00838f", "#b2ebf2"),
    ("#fbe9e7", "#d84315", "#ffccbc"),
];

fn module_colors_by_index(index: usize) -> (&'static str, &'static str, &'static str) {
    PALETTE[index % PALETTE.len()]
}

fn module_edge_color_by_index(index: usize) -> &'static str {
    PALETTE[index % PALETTE.len()].1
}

// ─── Phase 1: Module discovery ───────────────────────────────────────────────

fn discover_modules(src_dir: &Path) -> Vec<(String, Option<String>, PathBuf)> {
    let lib_path = src_dir.join("lib.rs");
    let content = fs::read_to_string(&lib_path).expect("Failed to read src/lib.rs");
    let syntax = syn::parse_file(&content).expect("Failed to parse src/lib.rs");

    let mut modules = Vec::new();
    let mut i = 0;
    let items: Vec<&syn::Item> = syntax.items.iter().collect();

    while i < items.len() {
        // Check for #[cfg(feature = "...")] on the next item
        let feature_gate = if let syn::Item::Mod(item_mod) = items[i] {
            extract_feature_gate(&item_mod.attrs)
        } else {
            None
        };

        if let syn::Item::Mod(item_mod) = items[i] {
            let mod_name = item_mod.ident.to_string();
            let mod_path = src_dir.join(format!("{}.rs", mod_name));
            if mod_path.exists() {
                modules.push((mod_name, feature_gate, mod_path));
            }
        }

        i += 1;
    }

    modules
}

fn extract_feature_gate(attrs: &[syn::Attribute]) -> Option<String> {
    for attr in attrs {
        if attr.path().is_ident("cfg") {
            let tokens = attr.meta.require_list().ok()?.tokens.to_string();
            // Parse: feature = "python"
            if let Some(pos) = tokens.find("feature") {
                let rest = &tokens[pos..];
                if let Some(start) = rest.find('"') {
                    let rest = &rest[start + 1..];
                    if let Some(end) = rest.find('"') {
                        return Some(rest[..end].to_string());
                    }
                }
            }
        }
    }
    None
}

// ─── Phase 2: Parse & index ──────────────────────────────────────────────────

fn parse_module(name: &str, path: &Path) -> ModuleInfo {
    let content = fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("Failed to read {}: {}", path.display(), e));
    let syntax = syn::parse_file(&content)
        .unwrap_or_else(|e| panic!("Failed to parse {}: {}", path.display(), e));

    let mut types = Vec::new();

    for item in &syntax.items {
        match item {
            syn::Item::Struct(s) => {
                let fields = extract_struct_fields(s);
                types.push(TypeInfo {
                    name: s.ident.to_string(),
                    kind: TypeKind::Struct,
                    fields,
                });
            }
            syn::Item::Trait(t) => {
                let fields = extract_trait_items(t);
                types.push(TypeInfo {
                    name: t.ident.to_string(),
                    kind: TypeKind::Trait,
                    fields,
                });
            }
            syn::Item::Enum(e) => {
                let fields = extract_enum_variants(e);
                types.push(TypeInfo {
                    name: e.ident.to_string(),
                    kind: TypeKind::Enum,
                    fields,
                });
            }
            _ => {}
        }
    }

    ModuleInfo {
        name: name.to_string(),
        feature_gate: None, // filled in later
        types,
        imports: HashMap::new(), // filled in phase 3
    }
}

fn extract_struct_fields(s: &syn::ItemStruct) -> Vec<(String, String)> {
    let mut fields = Vec::new();
    match &s.fields {
        syn::Fields::Named(named) => {
            for f in &named.named {
                if let Some(ident) = &f.ident {
                    let ty = type_to_short_string(&f.ty);
                    fields.push((ident.to_string(), ty));
                }
            }
        }
        syn::Fields::Unnamed(unnamed) => {
            for (i, f) in unnamed.unnamed.iter().enumerate() {
                let ty = type_to_short_string(&f.ty);
                fields.push((format!("{}", i), ty));
            }
        }
        syn::Fields::Unit => {}
    }
    fields
}

fn extract_trait_items(t: &syn::ItemTrait) -> Vec<(String, String)> {
    let mut items = Vec::new();

    // Extract associated types
    for item in &t.items {
        if let syn::TraitItem::Type(assoc) = item {
            let bounds: Vec<String> = assoc.bounds.iter().map(|b| quote_to_string(b)).collect();
            items.push((assoc.ident.to_string(), bounds.join(" + ")));
        }
    }

    // Extract method signatures (just name + simplified sig)
    for item in &t.items {
        if let syn::TraitItem::Fn(method) = item {
            let sig = method_sig_short(&method.sig);
            items.push((method.sig.ident.to_string(), sig));
        }
    }

    items
}

fn extract_enum_variants(e: &syn::ItemEnum) -> Vec<(String, String)> {
    let mut variants = Vec::new();
    for v in &e.variants {
        let fields_desc = match &v.fields {
            syn::Fields::Named(named) => {
                let parts: Vec<String> = named
                    .named
                    .iter()
                    .filter_map(|f| {
                        f.ident
                            .as_ref()
                            .map(|id| format!("{}: {}", id, type_to_short_string(&f.ty)))
                    })
                    .collect();
                format!("{{ {} }}", parts.join(", "))
            }
            syn::Fields::Unnamed(unnamed) => {
                let parts: Vec<String> = unnamed
                    .unnamed
                    .iter()
                    .map(|f| type_to_short_string(&f.ty))
                    .collect();
                format!("({})", parts.join(", "))
            }
            syn::Fields::Unit => String::new(),
        };
        variants.push((v.ident.to_string(), fields_desc));
    }
    variants
}

fn method_sig_short(sig: &syn::Signature) -> String {
    let params: Vec<String> = sig
        .inputs
        .iter()
        .filter_map(|arg| match arg {
            syn::FnArg::Receiver(_) => Some("&self".to_string()),
            syn::FnArg::Typed(pat) => Some(type_to_short_string(&pat.ty)),
        })
        .collect();
    let ret = match &sig.output {
        syn::ReturnType::Default => String::new(),
        syn::ReturnType::Type(_, ty) => format!(" → {}", type_to_short_string(ty)),
    };
    format!("({}){}", params.join(", "), ret)
}

fn type_to_short_string(ty: &syn::Type) -> String {
    // Produce a compact but readable type representation
    match ty {
        syn::Type::Path(tp) => {
            let segments: Vec<String> = tp
                .path
                .segments
                .iter()
                .map(|seg| {
                    let name = seg.ident.to_string();
                    match &seg.arguments {
                        syn::PathArguments::None => name,
                        syn::PathArguments::AngleBracketed(args) => {
                            let inner: Vec<String> = args
                                .args
                                .iter()
                                .map(|a| match a {
                                    syn::GenericArgument::Type(t) => type_to_short_string(t),
                                    syn::GenericArgument::Lifetime(lt) => {
                                        format!("'{}", lt.ident)
                                    }
                                    _ => quote_to_string(a),
                                })
                                .collect();
                            format!("{}<{}>", name, inner.join(", "))
                        }
                        syn::PathArguments::Parenthesized(args) => {
                            let inner: Vec<String> =
                                args.inputs.iter().map(type_to_short_string).collect();
                            format!("{}({})", name, inner.join(", "))
                        }
                    }
                })
                .collect();
            segments.join("::")
        }
        syn::Type::Reference(r) => {
            let lt = r
                .lifetime
                .as_ref()
                .map(|l| format!("&'{} ", l.ident))
                .unwrap_or_else(|| "&".to_string());
            let mutability = if r.mutability.is_some() { "mut " } else { "" };
            format!("{}{}{}", lt, mutability, type_to_short_string(&r.elem))
        }
        syn::Type::TraitObject(to) => {
            let bounds: Vec<String> = to.bounds.iter().map(|b| quote_to_string(b)).collect();
            format!("dyn {}", bounds.join(" + "))
        }
        syn::Type::Tuple(t) => {
            let inner: Vec<String> = t.elems.iter().map(type_to_short_string).collect();
            format!("({})", inner.join(", "))
        }
        syn::Type::Slice(s) => {
            format!("[{}]", type_to_short_string(&s.elem))
        }
        syn::Type::Array(a) => {
            format!("[{}; ..]", type_to_short_string(&a.elem))
        }
        _ => quote_to_string(ty),
    }
}

fn quote_to_string<T: quote::ToTokens>(t: &T) -> String {
    t.to_token_stream().to_string()
}

// ─── Phase 3: Import resolution ──────────────────────────────────────────────

fn resolve_imports(modules: &mut [ModuleInfo], src_dir: &Path) {
    // Build type_name → module_name lookup from all modules
    let mut type_to_module: HashMap<String, String> = HashMap::new();
    for module in modules.iter() {
        for ty in &module.types {
            type_to_module.insert(ty.name.clone(), module.name.clone());
        }
    }

    // For each module, parse its use items and resolve imports
    for module in modules.iter_mut() {
        let path = src_dir.join(format!("{}.rs", module.name));
        let content = fs::read_to_string(&path).unwrap();
        let syntax = syn::parse_file(&content).unwrap();

        for item in &syntax.items {
            if let syn::Item::Use(use_item) = item {
                collect_use_imports(&use_item.tree, &[], &mut module.imports);
            }
        }
    }
}

fn collect_use_imports(
    tree: &syn::UseTree,
    prefix: &[String],
    imports: &mut HashMap<String, (String, String)>,
) {
    match tree {
        syn::UseTree::Path(p) => {
            let mut new_prefix = prefix.to_vec();
            new_prefix.push(p.ident.to_string());
            collect_use_imports(&p.tree, &new_prefix, imports);
        }
        syn::UseTree::Name(n) => {
            let name = n.ident.to_string();
            if let Some(module) = extract_crate_module(prefix) {
                imports.insert(name.clone(), (module, name));
            }
        }
        syn::UseTree::Rename(r) => {
            let original = r.ident.to_string();
            let alias = r.rename.to_string();
            if let Some(module) = extract_crate_module(prefix) {
                imports.insert(alias, (module, original));
            }
        }
        syn::UseTree::Glob(_) => {
            // `use crate::foo::*` — we skip glob imports
        }
        syn::UseTree::Group(g) => {
            for tree in &g.items {
                collect_use_imports(tree, prefix, imports);
            }
        }
    }
}

/// Given a use path prefix like ["crate", "actor"], return the module name "actor".
/// Returns None for non-crate paths (std, external crates).
fn extract_crate_module(prefix: &[String]) -> Option<String> {
    if prefix.first().map(|s| s.as_str()) == Some("crate") {
        prefix.get(1).cloned()
    } else {
        None
    }
}

// ─── Phase 4: Dependency extraction ──────────────────────────────────────────

fn extract_edges(modules: &[ModuleInfo], src_dir: &Path) -> Vec<Edge> {
    let mut edges = Vec::new();

    // Build type_name → module_name lookup
    let mut type_to_module: HashMap<String, String> = HashMap::new();
    for module in modules {
        for ty in &module.types {
            type_to_module.insert(ty.name.clone(), module.name.clone());
        }
    }

    for module in modules {
        // Parse file again for impl blocks
        let path = src_dir.join(format!("{}.rs", module.name));
        let content = fs::read_to_string(&path).unwrap();
        let syntax = syn::parse_file(&content).unwrap();

        // Extract edges from struct/trait/enum fields
        for ty in &module.types {
            for (_field_name, field_type) in &ty.fields {
                let referenced = extract_type_names_from_string(field_type);
                for ref_name in &referenced {
                    if ref_name == &ty.name {
                        continue; // skip self-references
                    }
                    if let Some(target_module) = resolve_type(ref_name, module, &type_to_module) {
                        edges.push(Edge {
                            from_module: module.name.clone(),
                            from_type: ty.name.clone(),
                            to_module: target_module.clone(),
                            to_type: ref_name.clone(),
                            kind: EdgeKind::Field,
                            label: _field_name.clone(),
                        });
                    }
                }
            }
        }

        // Extract edges from impl blocks
        for item in &syntax.items {
            if let syn::Item::Impl(impl_block) = item {
                let self_type = extract_base_type_name(&impl_block.self_ty);
                if self_type.is_none() {
                    continue;
                }
                let self_type = self_type.unwrap();
                let self_module = type_to_module.get(&self_type).cloned();

                // Skip generic/blanket impls (self type is a type parameter, not a known type)
                if self_module.is_none() {
                    continue;
                }

                // Trait impl: `impl Trait for Type`
                if let Some((_, trait_path, _)) = &impl_block.trait_ {
                    let trait_name = path_to_name(trait_path);
                    if trait_name == self_type {
                        // skip self-impl (e.g. blanket impls)
                    } else if is_std_type(&trait_name) {
                        // skip std trait impls (Send, Sync, Clone, etc.)
                    } else if let Some(target_module) =
                        resolve_type(&trait_name, module, &type_to_module)
                    {
                        // Attribute to the module where the self type lives
                        let from_mod = self_module.clone().unwrap_or(module.name.clone());
                        edges.push(Edge {
                            from_module: from_mod,
                            from_type: self_type.clone(),
                            to_module: target_module,
                            to_type: trait_name.clone(),
                            kind: EdgeKind::TraitImpl,
                            label: "impl".to_string(),
                        });
                    }
                }

                // Only process method signatures for types belonging to this module
                if self_module.as_deref() != Some(&module.name) {
                    continue;
                }

                // Method signatures — extract types from params/return types
                for impl_item in &impl_block.items {
                    if let syn::ImplItem::Fn(method) = impl_item {
                        let sig_types = extract_types_from_sig(&method.sig);
                        for ref_name in &sig_types {
                            if ref_name == &self_type {
                                continue;
                            }
                            if let Some(target_module) =
                                resolve_type(ref_name, module, &type_to_module)
                            {
                                let label = format!(
                                    "{}() param",
                                    method.sig.ident
                                );
                                edges.push(Edge {
                                    from_module: module.name.clone(),
                                    from_type: self_type.clone(),
                                    to_module: target_module,
                                    to_type: ref_name.clone(),
                                    kind: EdgeKind::Field,
                                    label,
                                });
                            }
                        }
                    }
                }
            }
        }

        // Extract edges from trait definitions (method params referencing other types)
        for item in &syntax.items {
            if let syn::Item::Trait(trait_def) = item {
                let trait_name = trait_def.ident.to_string();
                if type_to_module.get(&trait_name) != Some(&module.name) {
                    continue;
                }

                for trait_item in &trait_def.items {
                    if let syn::TraitItem::Fn(method) = trait_item {
                        let sig_types = extract_types_from_sig(&method.sig);
                        for ref_name in &sig_types {
                            if ref_name == &trait_name {
                                continue;
                            }
                            if let Some(target_module) =
                                resolve_type(ref_name, module, &type_to_module)
                            {
                                let label = format!(
                                    "{}() param",
                                    method.sig.ident
                                );
                                edges.push(Edge {
                                    from_module: module.name.clone(),
                                    from_type: trait_name.clone(),
                                    to_module: target_module,
                                    to_type: ref_name.clone(),
                                    kind: EdgeKind::Field,
                                    label,
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    // Deduplicate edges
    dedup_edges(&mut edges);
    edges
}

fn dedup_edges(edges: &mut Vec<Edge>) {
    let mut seen = std::collections::HashSet::new();
    edges.retain(|e| {
        let key = (
            e.from_module.clone(),
            e.from_type.clone(),
            e.to_module.clone(),
            e.to_type.clone(),
            e.kind.clone(),
        );
        seen.insert(key)
    });
}

/// Extract all type names referenced in a method signature
fn extract_types_from_sig(sig: &syn::Signature) -> Vec<String> {
    let mut types = Vec::new();
    for arg in &sig.inputs {
        match arg {
            syn::FnArg::Typed(pat_type) => {
                collect_type_names(&pat_type.ty, &mut types);
            }
            _ => {}
        }
    }
    if let syn::ReturnType::Type(_, ty) = &sig.output {
        collect_type_names(ty, &mut types);
    }
    types
}

/// Recursively collect type names from a syn::Type
fn collect_type_names(ty: &syn::Type, names: &mut Vec<String>) {
    match ty {
        syn::Type::Path(tp) => {
            for seg in &tp.path.segments {
                let name = seg.ident.to_string();
                // Skip standard library / primitive wrappers
                if !is_std_wrapper(&name) && !is_primitive(&name) {
                    names.push(name.clone());
                }
                if let syn::PathArguments::AngleBracketed(args) = &seg.arguments {
                    for arg in &args.args {
                        if let syn::GenericArgument::Type(inner) = arg {
                            collect_type_names(inner, names);
                        }
                    }
                }
            }
        }
        syn::Type::Reference(r) => {
            collect_type_names(&r.elem, names);
        }
        syn::Type::TraitObject(to) => {
            for bound in &to.bounds {
                if let syn::TypeParamBound::Trait(t) = bound {
                    if let Some(seg) = t.path.segments.last() {
                        let name = seg.ident.to_string();
                        if !is_std_type(&name) {
                            names.push(name);
                        }
                    }
                }
            }
        }
        syn::Type::Tuple(t) => {
            for elem in &t.elems {
                collect_type_names(elem, names);
            }
        }
        syn::Type::Slice(s) => {
            collect_type_names(&s.elem, names);
        }
        syn::Type::Paren(p) => {
            collect_type_names(&p.elem, names);
        }
        _ => {}
    }
}

/// Given a short type name and a module's import map, resolve to the source module.
fn resolve_type(
    name: &str,
    module: &ModuleInfo,
    type_to_module: &HashMap<String, String>,
) -> Option<String> {
    // Check import map first
    if let Some((src_module, _original)) = module.imports.get(name) {
        // Verify the type actually exists in that module
        if type_to_module.contains_key(name) {
            return Some(src_module.clone());
        }
        // The import pointed to a module, but the type name from the import
        // might be the original name
        if type_to_module.contains_key(_original) {
            return Some(src_module.clone());
        }
    }

    // Check if type is defined in any module
    type_to_module.get(name).cloned()
}

fn extract_base_type_name(ty: &syn::Type) -> Option<String> {
    match ty {
        syn::Type::Path(tp) => {
            tp.path.segments.last().map(|s| s.ident.to_string())
        }
        _ => None,
    }
}

fn path_to_name(path: &syn::Path) -> String {
    path.segments
        .last()
        .map(|s| s.ident.to_string())
        .unwrap_or_default()
}

fn extract_type_names_from_string(type_str: &str) -> Vec<String> {
    // Extract PascalCase type names from a type string
    let mut names = Vec::new();
    let mut current = String::new();

    for ch in type_str.chars() {
        if ch.is_alphanumeric() || ch == '_' {
            current.push(ch);
        } else {
            if !current.is_empty() {
                if is_pascal_case(&current)
                    && !is_std_wrapper(&current)
                    && !is_primitive(&current)
                    && !is_std_type(&current)
                {
                    names.push(current.clone());
                }
                current.clear();
            }
        }
    }
    if !current.is_empty()
        && is_pascal_case(&current)
        && !is_std_wrapper(&current)
        && !is_primitive(&current)
        && !is_std_type(&current)
    {
        names.push(current);
    }

    names
}

fn is_pascal_case(s: &str) -> bool {
    s.len() > 1 && s.chars().next().map(|c| c.is_uppercase()).unwrap_or(false)
}

fn is_std_wrapper(name: &str) -> bool {
    matches!(
        name,
        "Arc" | "Box"
            | "Option"
            | "Vec"
            | "HashMap"
            | "HashSet"
            | "RwLock"
            | "Mutex"
            | "RefCell"
            | "Cell"
            | "Rc"
            | "Result"
            | "VecDeque"
            | "BTreeMap"
            | "BTreeSet"
            | "AtomicBool"
            | "AtomicUsize"
            | "AtomicI64"
            | "JoinHandle"
            | "Ordering"
    )
}

fn is_primitive(name: &str) -> bool {
    matches!(
        name,
        "bool" | "u8"
            | "u16"
            | "u32"
            | "u64"
            | "u128"
            | "usize"
            | "i8"
            | "i16"
            | "i32"
            | "i64"
            | "i128"
            | "isize"
            | "f32"
            | "f64"
            | "str"
            | "String"
            | "Self"
    )
}

fn is_std_type(name: &str) -> bool {
    matches!(
        name,
        "Any" | "Send"
            | "Sync"
            | "Sized"
            | "Clone"
            | "Copy"
            | "Debug"
            | "Display"
            | "Default"
            | "Hash"
            | "Eq"
            | "PartialEq"
            | "Ord"
            | "PartialOrd"
            | "From"
            | "Into"
            | "AsRef"
            | "Iterator"
            | "IntoIterator"
            | "ToString"
            | "Hasher"
            | "PyObject"
            | "PyResult"
            | "PyErr"
            | "PyModule"
            | "Python"
            | "Bound"
            | "PyAny"
            | "ArrayQueue"
            | "SegQueue"
    )
}

// ─── Phase 5: DOT output ─────────────────────────────────────────────────────

fn generate_dot(modules: &[ModuleInfo], edges: &[Edge]) -> String {
    let mut out = String::new();

    writeln!(out, "digraph swactor {{").unwrap();
    writeln!(out, "    rankdir=LR;").unwrap();
    writeln!(out, "    fontname=\"Helvetica\";").unwrap();
    writeln!(out, "    fontsize=14;").unwrap();
    writeln!(
        out,
        "    node [fontname=\"Helvetica\", fontsize=11, style=filled, shape=record];"
    )
    .unwrap();
    writeln!(out, "    edge [fontname=\"Helvetica\", fontsize=9];").unwrap();
    writeln!(out, "    label=\"swactor — internal dependency DAG\";").unwrap();
    writeln!(out, "    labelloc=t;").unwrap();
    writeln!(out, "    compound=true;").unwrap();
    writeln!(out, "    newrank=true;").unwrap();
    writeln!(out, "    splines=ortho;").unwrap();
    writeln!(out).unwrap();

    // Use actual module names in discovery order for consistent output
    let module_order: Vec<&str> = modules.iter().map(|m| m.name.as_str()).collect();

    // Build module_name → index lookup for palette rotation
    let module_index: HashMap<&str, usize> = module_order
        .iter()
        .enumerate()
        .map(|(i, &name)| (name, i))
        .collect();

    // Emit subgraph clusters
    for (i, mod_name) in module_order.iter().enumerate() {
        if let Some(module) = modules.iter().find(|m| m.name == *mod_name) {
            let (cluster_fill, cluster_border, node_fill) = module_colors_by_index(i);
            emit_cluster(&mut out, module, cluster_fill, cluster_border, node_fill);
        }
    }

    // Emit intra-module edges (within same cluster)
    writeln!(out).unwrap();
    writeln!(
        out,
        "    // ═══════════════════════════════════════════════════════════════════"
    )
    .unwrap();
    writeln!(
        out,
        "    //  INTRA-MODULE EDGES (within same cluster)"
    )
    .unwrap();
    writeln!(
        out,
        "    // ═══════════════════════════════════════════════════════════════════"
    )
    .unwrap();
    writeln!(out).unwrap();

    for edge in edges.iter().filter(|e| e.from_module == e.to_module) {
        let idx = module_index.get(edge.from_module.as_str()).copied().unwrap_or(0);
        emit_edge(&mut out, edge, true, module_edge_color_by_index(idx));
    }

    // Emit cross-module edges
    writeln!(out).unwrap();
    writeln!(
        out,
        "    // ═══════════════════════════════════════════════════════════════════"
    )
    .unwrap();
    writeln!(
        out,
        "    //  CROSS-MODULE EDGES  (the real dependency DAG)"
    )
    .unwrap();
    writeln!(
        out,
        "    // ═══════════════════════════════════════════════════════════════════"
    )
    .unwrap();

    // Group cross-module edges by (from_module, to_module)
    let mut grouped: HashMap<(String, String), Vec<&Edge>> = HashMap::new();
    for edge in edges.iter().filter(|e| e.from_module != e.to_module) {
        grouped
            .entry((edge.from_module.clone(), edge.to_module.clone()))
            .or_default()
            .push(edge);
    }

    // Sort groups by module order for deterministic output
    let mut group_keys: Vec<(String, String)> = grouped.keys().cloned().collect();
    group_keys.sort_by(|a, b| {
        let ai = module_order
            .iter()
            .position(|m| *m == a.0)
            .unwrap_or(99);
        let bi = module_order
            .iter()
            .position(|m| *m == b.0)
            .unwrap_or(99);
        let aj = module_order
            .iter()
            .position(|m| *m == a.1)
            .unwrap_or(99);
        let bj = module_order
            .iter()
            .position(|m| *m == b.1)
            .unwrap_or(99);
        (ai, aj).cmp(&(bi, bj))
    });

    for key in &group_keys {
        let edges_group = &grouped[key];
        writeln!(out).unwrap();
        writeln!(
            out,
            "    // --- {} depends on {} ---",
            key.0, key.1
        )
        .unwrap();
        for edge in edges_group {
            let idx = module_index.get(edge.from_module.as_str()).copied().unwrap_or(0);
            emit_edge(&mut out, edge, false, module_edge_color_by_index(idx));
        }
    }

    writeln!(out, "}}").unwrap();
    out
}

fn emit_cluster(out: &mut String, module: &ModuleInfo, cluster_fill: &str, cluster_border: &str, node_fill: &str) {

    let style = if module.feature_gate.is_some() {
        "rounded,dashed,filled"
    } else {
        "rounded,filled"
    };

    let label = if module.feature_gate.is_some() {
        format!("{}  (feature-gated)", module.name)
    } else {
        module.name.clone()
    };

    writeln!(
        out,
        "    subgraph cluster_{} {{",
        module.name
    )
    .unwrap();
    writeln!(out, "        label=\"{}\";", label).unwrap();
    writeln!(
        out,
        "        style=\"{}\"; fillcolor=\"{}\"; color=\"{}\";",
        style, cluster_fill, cluster_border
    )
    .unwrap();

    for ty in &module.types {
        let prefix = match ty.kind {
            TypeKind::Trait => "«trait» ",
            TypeKind::Enum => "«enum» ",
            TypeKind::Struct => "",
        };

        let fields_str = if ty.fields.is_empty() {
            String::new()
        } else {
            let field_lines: Vec<String> = ty
                .fields
                .iter()
                .map(|(name, ty_desc)| {
                    if ty_desc.is_empty() {
                        escape_dot(name)
                    } else if ty.kind == TypeKind::Trait {
                        // For traits, show method signatures
                        format!("{}({})", escape_dot(name), escape_dot(ty_desc))
                    } else if ty.kind == TypeKind::Enum {
                        // For enum variants, show variant name and fields
                        if ty_desc.is_empty() {
                            escape_dot(name)
                        } else {
                            format!("{} {}", escape_dot(name), escape_dot(ty_desc))
                        }
                    } else {
                        format!("{}: {}", escape_dot(name), escape_dot(ty_desc))
                    }
                })
                .collect();
            format!("|{}", field_lines.join("\\n"))
        };

        writeln!(
            out,
            "        {} [label=\"{{{}{}{}}}\", fillcolor=\"{}\"];",
            ty.name, prefix, ty.name, fields_str, node_fill
        )
        .unwrap();
    }

    writeln!(out, "    }}").unwrap();
}

fn emit_edge(out: &mut String, edge: &Edge, intra: bool, color: &str) {

    let (style, penwidth) = match edge.kind {
        EdgeKind::TraitImpl => {
            if intra {
                ("dotted", "1")
            } else {
                ("dotted", "1.5")
            }
        }
        EdgeKind::TraitObject => {
            if intra {
                ("dashed", "1")
            } else {
                ("dashed", "1.5")
            }
        }
        EdgeKind::Field => {
            if intra {
                ("dashed", "1")
            } else {
                ("solid", "1.5")
            }
        }
    };

    let label_escaped = escape_dot(&edge.label);

    writeln!(
        out,
        "    {} -> {} [label=\"{}\", style={}, color=\"{}\", penwidth={}];",
        edge.from_type, edge.to_type, label_escaped, style, color, penwidth
    )
    .unwrap();
}

fn escape_dot(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('<', "\\<")
        .replace('>', "\\>")
        .replace('{', "\\{")
        .replace('}', "\\}")
        .replace('|', "\\|")
}

// ─── Phase 6: HTML output ────────────────────────────────────────────────────

fn generate_html(dot_source: &str) -> String {
    // Escape the DOT source for embedding in a JS template literal
    let dot_escaped = dot_source
        .replace('\\', "\\\\")
        .replace('`', "\\`")
        .replace("${", "\\${");

    format!(
        r##"<!DOCTYPE html>
<html><head>
<meta charset='utf-8'>
<title>swactor dependency DAG</title>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ background:#1a1a2e; overflow:hidden; font-family:system-ui; }}
  #controls {{ position:fixed; top:12px; left:12px; z-index:10;
              background:rgba(30,30,60,0.9); border-radius:8px; padding:12px;
              color:#ccc; font-size:13px; backdrop-filter:blur(8px); }}
  #controls button {{ background:#333; color:#fff; border:1px solid #555;
                     border-radius:4px; padding:4px 10px; cursor:pointer; margin:0 3px; }}
  #controls button:hover {{ background:#555; }}
  #viewport {{ width:100vw; height:100vh; cursor:grab; }}
  #viewport:active {{ cursor:grabbing; }}
  #loading {{ position:fixed; top:50%; left:50%; transform:translate(-50%,-50%);
              color:#ccc; font-size:18px; }}
  svg {{ display:block; }}
</style>
</head><body>
<div id='controls'>
  <strong>swactor dep graph</strong> &nbsp;
  <button onclick='zoomIn()'>+</button>
  <button onclick='zoomOut()'>&minus;</button>
  <button onclick='resetView()'>fit</button>
  <span style='margin-left:8px;opacity:0.6'>scroll to zoom · drag to pan · click node to focus</span>
</div>
<div id='viewport'></div>
<div id='loading'>Loading Graphviz…</div>
<script type="module">
import {{ instance }} from 'https://cdn.jsdelivr.net/npm/@viz-js/viz@3.11.0/lib/viz-standalone.mjs';

const dot = `{dot_escaped}`;

const viz = await instance();
const svg = viz.renderSVGElement(dot);
document.getElementById('loading').remove();

const vp = document.getElementById('viewport');
vp.appendChild(svg);

// invert colors for dark mode
svg.querySelectorAll('polygon[fill="white"]').forEach(el => el.setAttribute('fill','#1a1a2e'));

// Recolor text: node text stays dark (readable on light fills), everything else goes light
svg.querySelectorAll('.graph > text, .cluster > text, .edge text').forEach(el => el.setAttribute('fill','#e0e0e0'));
// Node text (inside record shapes): keep dark for readability on pastel fills
svg.querySelectorAll('.node text').forEach(el => el.setAttribute('fill','#1a1a1a'));

// ─── Click-to-focus ────────────────────────────────────────────────────────
// Build adjacency: for each edge, record which node titles it connects.
const edges = svg.querySelectorAll('.edge');
const nodes = svg.querySelectorAll('.node');
// Cluster chrome = the path + text that draw the cluster box/label (not child nodes)
const clusterChrome = [];
svg.querySelectorAll('.cluster').forEach(c => {{
  c.querySelectorAll(':scope > path, :scope > polygon, :scope > text').forEach(el => clusterChrome.push(el));
}});

// Map: node title → DOM element
const nodeByTitle = new Map();
nodes.forEach(n => {{
  const t = n.querySelector('title');
  if (t) nodeByTitle.set(t.textContent.trim(), n);
}});

// Which cluster contains which node titles
const nodeToClusterEls = new Map();
svg.querySelectorAll('.cluster').forEach(cluster => {{
  const chrome = [...cluster.querySelectorAll(':scope > path, :scope > polygon, :scope > text')];
  cluster.querySelectorAll('.node title').forEach(t => {{
    nodeToClusterEls.set(t.textContent.trim(), chrome);
  }});
}});

// Map: node title → set of connected edge elements + set of neighbor titles
const adj = new Map();
edges.forEach(edge => {{
  const t = edge.querySelector('title');
  if (!t) return;
  const parts = t.textContent.trim().split('->').map(s => s.trim());
  if (parts.length !== 2) return;
  const [src, dst] = parts;
  if (!adj.has(src)) adj.set(src, {{ edges: [], neighbors: new Set() }});
  if (!adj.has(dst)) adj.set(dst, {{ edges: [], neighbors: new Set() }});
  adj.get(src).edges.push(edge);
  adj.get(src).neighbors.add(dst);
  adj.get(dst).edges.push(edge);
  adj.get(dst).neighbors.add(src);
}});

const DIM = 0.08;
let focused = null;

function clearFocus() {{
  focused = null;
  nodes.forEach(n => n.style.opacity = '');
  edges.forEach(e => e.style.opacity = '');
  clusterChrome.forEach(el => el.style.opacity = '');
}}

function focusNode(title) {{
  if (focused === title) {{ clearFocus(); return; }}
  focused = title;
  const info = adj.get(title) || {{ edges: [], neighbors: new Set() }};
  const connected = new Set([title, ...info.neighbors]);

  // Dim all nodes, edges, and cluster chrome individually (not the cluster <g>)
  nodes.forEach(n => n.style.opacity = DIM);
  edges.forEach(e => e.style.opacity = DIM);
  clusterChrome.forEach(el => el.style.opacity = DIM);

  // Highlight connected nodes
  connected.forEach(name => {{
    const el = nodeByTitle.get(name);
    if (el) el.style.opacity = 1;
  }});

  // Highlight connected edges
  info.edges.forEach(e => e.style.opacity = 1);

  // Highlight cluster chrome for clusters that contain a connected node
  const seen = new Set();
  connected.forEach(name => {{
    const chrome = nodeToClusterEls.get(name);
    if (chrome) chrome.forEach(el => {{
      if (!seen.has(el)) {{ seen.add(el); el.style.opacity = 1; }}
    }});
  }});
}}

// Attach click handlers to nodes
nodes.forEach(node => {{
  node.style.cursor = 'pointer';
  node.addEventListener('click', e => {{
    e.stopPropagation();
    const t = node.querySelector('title');
    if (t) focusNode(t.textContent.trim());
  }});
}});

// pan & zoom
let scale = 1, tx = 0, ty = 0, dragging = false, didDrag = false, sx = 0, sy = 0;
function applyTransform() {{ svg.style.transform = `translate(${{tx}}px,${{ty}}px) scale(${{scale}})`; svg.style.transformOrigin = '0 0'; }}
function resetView() {{
  const vw = window.innerWidth, vh = window.innerHeight;
  const bb = svg.getBBox();
  scale = Math.min(vw / bb.width, vh / bb.height) * 0.92;
  tx = (vw - bb.width * scale) / 2;
  ty = (vh - bb.height * scale) / 2;
  applyTransform();
}}
resetView();

vp.addEventListener('wheel', e => {{ e.preventDefault(); const f = e.deltaY < 0 ? 1.12 : 0.89; const rect = vp.getBoundingClientRect(); const mx = e.clientX - rect.left; const my = e.clientY - rect.top; tx = mx - f * (mx - tx); ty = my - f * (my - ty); scale *= f; applyTransform(); }}, {{ passive:false }});
vp.addEventListener('pointerdown', e => {{ dragging=true; didDrag=false; sx=e.clientX-tx; sy=e.clientY-ty; vp.setPointerCapture(e.pointerId); }});
vp.addEventListener('pointermove', e => {{ if(!dragging) return; didDrag=true; tx=e.clientX-sx; ty=e.clientY-sy; applyTransform(); }});
vp.addEventListener('pointerup', () => dragging=false);
// Click background to clear focus (only if it wasn't a drag)
vp.addEventListener('click', e => {{ if (!didDrag && !e.target.closest('.node')) clearFocus(); }});

function zoomIn() {{ scale*=1.3; applyTransform(); }}
function zoomOut() {{ scale*=0.7; applyTransform(); }}
</script>
</body></html>
"##
    )
}

// ─── Main ────────────────────────────────────────────────────────────────────

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut src_dir = PathBuf::from("src");
    let mut output_prefix = String::from("deps");
    let mut output_dir: Option<PathBuf> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--src-dir" => {
                i += 1;
                src_dir = PathBuf::from(&args[i]);
            }
            "--output" => {
                i += 1;
                output_prefix = args[i].clone();
            }
            "--output-dir" => {
                i += 1;
                output_dir = Some(PathBuf::from(&args[i]));
            }
            "--help" | "-h" => {
                eprintln!("Usage: depgraph [--src-dir src/] [--output deps] [--output-dir DIR]");
                eprintln!("  --src-dir DIR     Source directory (default: src/)");
                eprintln!("  --output PREFIX   Output prefix (default: deps)");
                eprintln!("  --output-dir DIR  Directory for output files (default: cwd)");
                eprintln!("                    Produces PREFIX.dot and PREFIX.html");
                std::process::exit(0);
            }
            other => {
                eprintln!("Unknown argument: {}", other);
                std::process::exit(1);
            }
        }
        i += 1;
    }

    // Ensure output directory exists.
    if let Some(ref dir) = output_dir {
        fs::create_dir_all(dir).expect("Failed to create output directory");
    }

    eprintln!("Scanning source directory: {}", src_dir.display());

    // Phase 1: Module discovery
    let module_defs = discover_modules(&src_dir);
    eprintln!(
        "Found {} modules: {}",
        module_defs.len(),
        module_defs
            .iter()
            .map(|(n, _, _)| n.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Phase 2: Parse & index
    let mut modules: Vec<ModuleInfo> = module_defs
        .iter()
        .map(|(name, feature, path)| {
            let mut m = parse_module(name, path);
            m.feature_gate = feature.clone();
            m
        })
        .collect();

    for m in &modules {
        eprintln!(
            "  {} — {} types: {}",
            m.name,
            m.types.len(),
            m.types
                .iter()
                .map(|t| t.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    // Phase 3: Import resolution
    resolve_imports(&mut modules, &src_dir);

    // Phase 4: Dependency extraction
    let edges = extract_edges(&modules, &src_dir);
    eprintln!("Found {} dependency edges", edges.len());

    let cross_module = edges
        .iter()
        .filter(|e| e.from_module != e.to_module)
        .count();
    let intra_module = edges.len() - cross_module;
    eprintln!(
        "  {} cross-module, {} intra-module",
        cross_module, intra_module
    );

    // Phase 5: DOT output
    let dot = generate_dot(&modules, &edges);
    let dot_file = format!("{}.dot", output_prefix);
    let dot_path = match &output_dir {
        Some(dir) => dir.join(&dot_file),
        None => PathBuf::from(&dot_file),
    };
    fs::write(&dot_path, &dot).expect("Failed to write .dot file");
    eprintln!("Wrote {}", dot_path.display());

    // Phase 6: HTML output
    let html = generate_html(&dot);
    let html_file = format!("{}.html", output_prefix);
    let html_path = match &output_dir {
        Some(dir) => dir.join(&html_file),
        None => PathBuf::from(&html_file),
    };
    fs::write(&html_path, &html).expect("Failed to write .html file");
    eprintln!("Wrote {}", html_path.display());

    eprintln!("Done!");
}
