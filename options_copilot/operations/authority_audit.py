"""Fail-closed static audit for broker write authority.

The audit deliberately works from Python syntax rather than imports.  It can
therefore inspect optional adapters without importing broker SDKs or executing
repository code.  Any callable that defines, acquires, or reaches a submit,
transmit, or order-placement capability is reported, including transitive
wrappers and inherited methods.
"""
from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass, field
import json
from pathlib import Path
import re
import tokenize
from types import MappingProxyType
from typing import Iterable, Mapping, Sequence


SCHEMA_VERSION = 1
_CAMEL_BOUNDARY = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")


@dataclass(frozen=True, slots=True)
class AuditViolation:
    path: str
    line: int
    column: int
    kind: str
    symbol: str
    target: str
    message: str
    trace: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {
            "path": self.path,
            "line": self.line,
            "column": self.column,
            "kind": self.kind,
            "symbol": self.symbol,
            "target": self.target,
            "message": self.message,
            "trace": list(self.trace),
        }


@dataclass(frozen=True, slots=True)
class AuthorityAuditReport:
    root: Path
    files_scanned: int
    callables_scanned: int
    unsafe_callables: tuple[str, ...]
    call_graph: Mapping[str, tuple[str, ...]]
    violations: tuple[AuditViolation, ...]

    @property
    def ok(self) -> bool:
        return not self.violations

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "ok": self.ok,
            "root": self.root.as_posix(),
            "files_scanned": self.files_scanned,
            "callables_scanned": self.callables_scanned,
            "unsafe_callables": list(self.unsafe_callables),
            "call_graph": {
                symbol: list(targets) for symbol, targets in self.call_graph.items()
            },
            "violations": [violation.as_dict() for violation in self.violations],
        }

    to_dict = as_dict


@dataclass(slots=True)
class _CallEdge:
    target: str
    line: int
    column: int


@dataclass(slots=True)
class _CallableInfo:
    symbol: str
    name: str
    node: ast.FunctionDef | ast.AsyncFunctionDef
    class_symbol: str | None
    path: str
    calls: list[_CallEdge] = field(default_factory=list)


@dataclass(slots=True)
class _ClassInfo:
    symbol: str
    name: str
    node: ast.ClassDef
    path: str
    bases: tuple[str, ...] = ()
    is_protocol: bool = False
    methods: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class _AliasExpression:
    name: str
    expression: ast.expr
    line: int
    column: int


@dataclass(slots=True)
class _Bindings:
    imports: dict[str, str] = field(default_factory=dict)
    constants: dict[str, str] = field(default_factory=dict)
    aliases: dict[str, str] = field(default_factory=dict)
    variables: set[str] = field(default_factory=set)
    import_nodes: list[tuple[str, ast.AST]] = field(default_factory=list)
    alias_expressions: list[_AliasExpression] = field(default_factory=list)


@dataclass(slots=True)
class _ModuleInfo:
    module: str
    path: Path
    relative_path: str
    tree: ast.Module
    is_package: bool
    parents: dict[ast.AST, ast.AST]
    bindings: _Bindings = field(default_factory=_Bindings)
    callables: dict[str, _CallableInfo] = field(default_factory=dict)
    classes: dict[str, _ClassInfo] = field(default_factory=dict)


@dataclass(slots=True)
class _ResolutionContext:
    module: _ModuleInfo
    callables: Mapping[str, _CallableInfo]
    classes: Mapping[str, _ClassInfo]
    imports: Mapping[str, str]
    constants: Mapping[str, str]
    aliases: Mapping[str, str]
    variables: set[str]
    current_symbol: str
    current_class: str | None


class _BindingCollector(ast.NodeVisitor):
    def __init__(self, module: _ModuleInfo, bindings: _Bindings) -> None:
        self.module = module
        self.bindings = bindings

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        return None

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        return None

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        return None

    def visit_Lambda(self, node: ast.Lambda) -> None:
        return None

    def visit_Import(self, node: ast.Import) -> None:
        for item in node.names:
            bound = item.asname or item.name.split(".", 1)[0]
            target = item.name if item.asname else item.name.split(".", 1)[0]
            self.bindings.imports[bound] = target
            self.bindings.import_nodes.append((target, node))

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        base = _resolve_import_module(self.module, node.module, node.level)
        for item in node.names:
            if item.name == "*":
                self.bindings.import_nodes.append((base + ".*", node))
                continue
            bound = item.asname or item.name
            target = ".".join(part for part in (base, item.name) if part)
            self.bindings.imports[bound] = target
            self.bindings.import_nodes.append((target, node))

    def visit_Assign(self, node: ast.Assign) -> None:
        for target in node.targets:
            self._assignment(target, node.value, node)
        self.visit(node.value)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            self._assignment(node.target, node.value, node)
            self.visit(node.value)

    def visit_NamedExpr(self, node: ast.NamedExpr) -> None:
        self._assignment(node.target, node.value, node)
        self.visit(node.value)

    def _assignment(self, target: ast.expr, value: ast.expr, node: ast.AST) -> None:
        names = tuple(_assigned_names(target))
        self.bindings.variables.update(names)
        if len(names) != 1:
            return
        name = names[0]
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            self.bindings.constants[name] = value.value
            return
        self.bindings.alias_expressions.append(
            _AliasExpression(
                name=name,
                expression=value,
                line=getattr(node, "lineno", 0),
                column=getattr(node, "col_offset", 0),
            )
        )


class _CallCollector(ast.NodeVisitor):
    def __init__(self, context: _ResolutionContext) -> None:
        self.context = context
        self.edges: list[_CallEdge] = []

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        return None

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        return None

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        return None

    def visit_Lambda(self, node: ast.Lambda) -> None:
        # Lambda bodies are executable and belong to the enclosing authority
        # boundary, even though the lambda is not independently addressable.
        self.visit(node.body)

    def visit_Call(self, node: ast.Call) -> None:
        for target in _resolve_call_targets(node.func, self.context):
            self.edges.append(
                _CallEdge(
                    target=target,
                    line=getattr(node, "lineno", 0),
                    column=getattr(node, "col_offset", 0),
                )
            )
        self.generic_visit(node)


def audit_repository(root: str | Path) -> AuthorityAuditReport:
    """Audit every Python file beneath *root* without importing any of them."""

    root_path = Path(root).resolve()
    if not root_path.is_dir():
        raise ValueError(f"authority audit root is not a directory: {root_path}")
    python_files = _production_python_files(root_path)
    modules: dict[str, _ModuleInfo] = {}
    violations: list[AuditViolation] = []

    for path in python_files:
        relative = path.relative_to(root_path).as_posix()
        try:
            with tokenize.open(path) as handle:
                source = handle.read()
            tree = ast.parse(source, filename=relative, type_comments=True)
        except (OSError, SyntaxError, UnicodeError) as exc:
            line = int(getattr(exc, "lineno", 0) or 0)
            column = int(getattr(exc, "offset", 0) or 0)
            module = _module_name(root_path, path)
            violations.append(
                AuditViolation(
                    path=relative,
                    line=line,
                    column=column,
                    kind="parse_error",
                    symbol=module,
                    target=relative,
                    message=f"Python source could not be parsed: {exc}",
                )
            )
            continue
        parents = {
            child: parent
            for parent in ast.walk(tree)
            for child in ast.iter_child_nodes(parent)
        }
        module = _module_name(root_path, path)
        info = _ModuleInfo(
            module=module,
            path=path,
            relative_path=relative,
            tree=tree,
            is_package=path.name == "__init__.py",
            parents=parents,
        )
        _index_definitions(info)
        modules[module] = info

    all_callables = {
        symbol: item
        for module in modules.values()
        for symbol, item in module.callables.items()
    }
    all_classes = {
        symbol: item
        for module in modules.values()
        for symbol, item in module.classes.items()
    }

    for module in modules.values():
        collector = _BindingCollector(module, module.bindings)
        collector.visit(module.tree)
        context = _context(
            module,
            all_callables,
            all_classes,
            module.bindings,
            current_symbol=module.module,
            current_class=None,
        )
        _resolve_aliases(module.bindings, context)

    _resolve_class_contracts(modules.values(), all_callables, all_classes)

    unsafe: dict[str, tuple[str, ...]] = {}
    for module in sorted(modules.values(), key=lambda item: item.relative_path):
        _record_forbidden_bindings(
            module,
            module.module,
            module.bindings,
            module.relative_path,
            violations,
            unsafe,
        )
        module_context = _context(
            module,
            all_callables,
            all_classes,
            module.bindings,
            current_symbol=module.module,
            current_class=None,
        )
        module_calls = _CallCollector(module_context)
        module_calls.visit(module.tree)
        _record_direct_calls(
            module.module,
            module.relative_path,
            module_calls.edges,
            violations,
            unsafe,
            include_in_unsafe=False,
        )

        for symbol, callable_info in sorted(module.callables.items()):
            class_info = (
                all_classes.get(callable_info.class_symbol)
                if callable_info.class_symbol is not None
                else None
            )
            if _is_forbidden_callable(callable_info.name):
                kind = (
                    "protocol_surface"
                    if class_info is not None and class_info.is_protocol
                    else "forbidden_definition"
                )
                violation = AuditViolation(
                    path=callable_info.path,
                    line=callable_info.node.lineno,
                    column=callable_info.node.col_offset,
                    kind=kind,
                    symbol=symbol,
                    target=symbol,
                    message="Callable exposes broker submit, transmit, or order-placement authority.",
                    trace=(symbol,),
                )
                violations.append(violation)
                unsafe.setdefault(symbol, (symbol,))

            local = _callable_bindings(module, callable_info)
            context = _context(
                module,
                all_callables,
                all_classes,
                local,
                current_symbol=symbol,
                current_class=callable_info.class_symbol,
            )
            _resolve_aliases(local, context)
            _record_forbidden_bindings(
                module,
                symbol,
                local,
                callable_info.path,
            violations,
            unsafe,
        )
            collector = _CallCollector(context)
            for statement in callable_info.node.body:
                collector.visit(statement)
            callable_info.calls.extend(collector.edges)
            _record_direct_calls(
                symbol,
                callable_info.path,
                callable_info.calls,
                violations,
                unsafe,
                include_in_unsafe=True,
            )

    _propagate_call_graph(all_callables, unsafe, violations)
    _propagate_inheritance(all_classes, unsafe, violations)
    _propagate_call_graph(all_callables, unsafe, violations)

    graph: dict[str, tuple[str, ...]] = {
        symbol: tuple(sorted({edge.target for edge in item.calls}))
        for symbol, item in sorted(all_callables.items())
    }
    for symbol in sorted(set(unsafe).difference(graph)):
        graph[symbol] = ()
    graph = dict(sorted(graph.items()))
    ordered_violations = _deduplicate_violations(violations)
    return AuthorityAuditReport(
        root=root_path,
        files_scanned=len(python_files),
        callables_scanned=len(all_callables),
        unsafe_callables=tuple(sorted(unsafe)),
        call_graph=MappingProxyType(graph),
        violations=ordered_violations,
    )


audit = audit_repository


def _production_python_files(root: Path) -> tuple[Path, ...]:
    package_root = root / "options_copilot"
    if package_root.is_dir() and (package_root / "__init__.py").is_file():
        # A repository-root audit owns the complete production package and
        # operator script trees. Virtual environments, tests, preserved
        # migrations, and generated evidence are not executable production
        # surfaces and contain intentional broker-authority fixtures.
        scan_roots = [package_root]
        scripts_root = root / "scripts"
        if scripts_root.is_dir():
            scan_roots.append(scripts_root)
    else:
        # Package-root and synthetic-fixture audits retain recursive behavior.
        scan_roots = [root]
    return tuple(
        sorted(
            (
                path
                for scan_root in scan_roots
                for path in scan_root.rglob("*.py")
                if path.is_file()
            ),
            key=lambda path: path.relative_to(root).as_posix(),
        )
    )


def _index_definitions(module: _ModuleInfo) -> None:
    for node in ast.walk(module.tree):
        if isinstance(node, ast.ClassDef):
            symbol = _qualified_node_symbol(module, node)
            module.classes[symbol] = _ClassInfo(
                symbol=symbol,
                name=node.name,
                node=node,
                path=module.relative_path,
            )
    for node in ast.walk(module.tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        symbol = _qualified_node_symbol(module, node)
        class_node = _nearest_parent(module, node, ast.ClassDef)
        class_symbol = (
            _qualified_node_symbol(module, class_node)
            if isinstance(class_node, ast.ClassDef)
            else None
        )
        info = _CallableInfo(
            symbol=symbol,
            name=node.name,
            node=node,
            class_symbol=class_symbol,
            path=module.relative_path,
        )
        module.callables[symbol] = info
        if isinstance(module.parents.get(node), ast.ClassDef) and class_symbol is not None:
            module.classes[class_symbol].methods[node.name] = symbol


def _resolve_class_contracts(
    modules: Iterable[_ModuleInfo],
    callables: Mapping[str, _CallableInfo],
    classes: Mapping[str, _ClassInfo],
) -> None:
    for module in modules:
        for info in module.classes.values():
            context = _context(
                module,
                callables,
                classes,
                module.bindings,
                current_symbol=info.symbol,
                current_class=info.symbol,
            )
            info.bases = tuple(
                target
                for base in info.node.bases
                if (target := _resolve_reference(base, context)) is not None
            )
            info.is_protocol = any(_is_protocol_name(base) for base in info.bases)
    changed = True
    while changed:
        changed = False
        for info in classes.values():
            if not info.is_protocol and any(
                classes.get(base) is not None and classes[base].is_protocol
                for base in info.bases
            ):
                info.is_protocol = True
                changed = True


def _callable_bindings(module: _ModuleInfo, info: _CallableInfo) -> _Bindings:
    bindings = _Bindings(
        imports=dict(module.bindings.imports),
        constants=dict(module.bindings.constants),
        aliases=dict(module.bindings.aliases),
        variables=set(module.bindings.variables),
    )
    arguments = info.node.args
    bindings.variables.update(argument.arg for argument in arguments.posonlyargs)
    bindings.variables.update(argument.arg for argument in arguments.args)
    bindings.variables.update(argument.arg for argument in arguments.kwonlyargs)
    if arguments.vararg is not None:
        bindings.variables.add(arguments.vararg.arg)
    if arguments.kwarg is not None:
        bindings.variables.add(arguments.kwarg.arg)
    collector = _BindingCollector(module, bindings)
    for statement in info.node.body:
        collector.visit(statement)
    return bindings


def _context(
    module: _ModuleInfo,
    callables: Mapping[str, _CallableInfo],
    classes: Mapping[str, _ClassInfo],
    bindings: _Bindings,
    *,
    current_symbol: str,
    current_class: str | None,
) -> _ResolutionContext:
    return _ResolutionContext(
        module=module,
        callables=callables,
        classes=classes,
        imports=bindings.imports,
        constants=bindings.constants,
        aliases=bindings.aliases,
        variables=bindings.variables,
        current_symbol=current_symbol,
        current_class=current_class,
    )


def _resolve_aliases(bindings: _Bindings, context: _ResolutionContext) -> None:
    unresolved = list(bindings.alias_expressions)
    for _ in range(len(unresolved) + 1):
        progress = False
        remaining: list[_AliasExpression] = []
        for item in unresolved:
            target = _resolve_reference(item.expression, context)
            if target is None:
                remaining.append(item)
                continue
            bindings.aliases[item.name] = target
            progress = True
        unresolved = remaining
        if not progress:
            break


def _record_forbidden_bindings(
    module: _ModuleInfo,
    symbol: str,
    bindings: _Bindings,
    path: str,
    violations: list[AuditViolation],
    unsafe: dict[str, tuple[str, ...]],
) -> None:
    # Callable binding sets copy resolved global maps, but their event lists
    # contain only syntax physically present inside that callable.
    imports = bindings.import_nodes
    for target, node in imports:
        if _target_is_forbidden(target):
            violations.append(
                AuditViolation(
                    path=path,
                    line=getattr(node, "lineno", 0),
                    column=getattr(node, "col_offset", 0),
                    kind="forbidden_import",
                    symbol=symbol,
                    target=target,
                    message="Import acquires broker write authority.",
                    trace=(symbol, target),
                )
            )
            if symbol in module.callables:
                unsafe.setdefault(symbol, (symbol, target))

    expressions = bindings.alias_expressions
    for item in expressions:
        target = bindings.aliases.get(item.name)
        if target is None or not _target_is_forbidden(target):
            continue
        violations.append(
            AuditViolation(
                path=path,
                line=item.line,
                column=item.column,
                kind="callable_alias",
                symbol=symbol,
                target=target,
                message="Alias acquires broker write authority.",
                trace=(symbol, target),
            )
        )
        if symbol in module.callables:
            unsafe.setdefault(symbol, (symbol, target))


def _record_direct_calls(
    symbol: str,
    path: str,
    edges: Sequence[_CallEdge],
    violations: list[AuditViolation],
    unsafe: dict[str, tuple[str, ...]],
    *,
    include_in_unsafe: bool,
) -> None:
    for edge in edges:
        if not _target_is_forbidden(edge.target):
            continue
        violations.append(
            AuditViolation(
                path=path,
                line=edge.line,
                column=edge.column,
                kind="direct_call",
                symbol=symbol,
                target=edge.target,
                message="Call reaches broker submit, transmit, or order-placement authority.",
                trace=(symbol, edge.target),
            )
        )
        if include_in_unsafe:
            unsafe.setdefault(symbol, (symbol, edge.target))


def _propagate_call_graph(
    callables: Mapping[str, _CallableInfo],
    unsafe: dict[str, tuple[str, ...]],
    violations: list[AuditViolation],
) -> None:
    changed = True
    while changed:
        changed = False
        for symbol, info in sorted(callables.items()):
            if symbol in unsafe:
                continue
            for edge in sorted(info.calls, key=lambda item: (item.line, item.column, item.target)):
                trace = unsafe.get(edge.target)
                if trace is None:
                    continue
                propagated = (symbol, *trace)
                unsafe[symbol] = propagated
                violations.append(
                    AuditViolation(
                        path=info.path,
                        line=edge.line,
                        column=edge.column,
                        kind="transitive_call",
                        symbol=symbol,
                        target=edge.target,
                        message="Callable transitively reaches broker write authority.",
                        trace=propagated,
                    )
                )
                changed = True
                break


def _propagate_inheritance(
    classes: Mapping[str, _ClassInfo],
    unsafe: dict[str, tuple[str, ...]],
    violations: list[AuditViolation],
) -> None:
    effective: dict[str, dict[str, str]] = {symbol: {} for symbol in classes}
    for symbol, class_info in classes.items():
        for name, method in class_info.methods.items():
            if method in unsafe:
                effective[symbol][name] = method
    changed = True
    while changed:
        changed = False
        for symbol, class_info in sorted(classes.items()):
            for base in class_info.bases:
                for name, base_method in effective.get(base, {}).items():
                    if name in class_info.methods or name in effective[symbol]:
                        continue
                    virtual = f"{symbol}.{name}"
                    base_trace = unsafe[base_method]
                    trace = (virtual, *base_trace)
                    effective[symbol][name] = virtual
                    unsafe[virtual] = trace
                    violations.append(
                        AuditViolation(
                            path=class_info.path,
                            line=class_info.node.lineno,
                            column=class_info.node.col_offset,
                            kind="inherited_authority",
                            symbol=symbol,
                            target=base_method,
                            message="Class inherits a callable with broker write authority.",
                            trace=trace,
                        )
                    )
                    changed = True


def _resolve_call_targets(
    expression: ast.expr, context: _ResolutionContext
) -> tuple[str, ...]:
    if (
        isinstance(expression, ast.Attribute)
        and isinstance(expression.value, ast.Call)
        and isinstance(expression.value.func, ast.Name)
        and expression.value.func.id == "super"
        and context.current_class is not None
    ):
        current = context.classes.get(context.current_class)
        if current is not None:
            targets = tuple(f"{base}.{expression.attr}" for base in current.bases)
            if targets:
                return targets
    target = _resolve_reference(expression, context)
    return () if target is None else (target,)


def _resolve_reference(
    expression: ast.expr, context: _ResolutionContext
) -> str | None:
    if isinstance(expression, ast.Name):
        name = expression.id
        if name in context.aliases:
            return context.aliases[name]
        if name in context.imports:
            return context.imports[name]
        if name in {"self", "cls"} and context.current_class is not None:
            return context.current_class
        target = _lookup_callable(name, context)
        if target is not None:
            return target
        class_target = _lookup_class(name, context)
        if class_target is not None:
            return class_target
        if name in context.variables:
            return None
        if name == "getattr":
            return "builtins.getattr"
        return f"*.{name}" if _is_forbidden_callable(name) else None

    if isinstance(expression, ast.Attribute):
        if (
            isinstance(expression.value, ast.Call)
            and isinstance(expression.value.func, ast.Name)
            and expression.value.func.id == "super"
        ):
            return None
        base = _resolve_reference(expression.value, context)
        if base is None:
            return f"*.{expression.attr}"
        if base.startswith("*."):
            return f"*.{expression.attr}"
        return f"{base}.{expression.attr}"

    if isinstance(expression, ast.Call) and _is_getattr_call(expression, context):
        attribute = _constant_string(expression.args[1], context.constants)
        if attribute is None:
            return None
        base = _resolve_reference(expression.args[0], context)
        if base is None or base.startswith("*."):
            return f"*.{attribute}"
        return f"{base}.{attribute}"

    return None


def _lookup_callable(name: str, context: _ResolutionContext) -> str | None:
    if context.current_class is not None:
        candidate = f"{context.current_class}.{name}"
        if candidate in context.callables:
            return candidate
    symbol_parts = context.current_symbol.split(".")
    while len(symbol_parts) > len(context.module.module.split(".")):
        candidate = ".".join((*symbol_parts, name))
        if candidate in context.callables:
            return candidate
        symbol_parts.pop()
    candidate = f"{context.module.module}.{name}"
    if candidate in context.callables:
        return candidate
    return None


def _lookup_class(name: str, context: _ResolutionContext) -> str | None:
    candidate = f"{context.module.module}.{name}"
    if candidate in context.classes:
        return candidate
    return None


def _is_getattr_call(node: ast.Call, context: _ResolutionContext) -> bool:
    if len(node.args) < 2:
        return False
    target = _resolve_reference(node.func, context)
    return target in {"builtins.getattr", None} and isinstance(node.func, ast.Name) and node.func.id == "getattr"


def _constant_string(expression: ast.expr, constants: Mapping[str, str]) -> str | None:
    if isinstance(expression, ast.Constant) and isinstance(expression.value, str):
        return expression.value
    if isinstance(expression, ast.Name):
        return constants.get(expression.id)
    return None


def _recorded_tokens(name: str) -> tuple[str, ...]:
    expanded = _CAMEL_BOUNDARY.sub("_", name)
    return tuple(
        token.casefold()
        for token in re.split(r"[^A-Za-z0-9]+", expanded)
        if token
    )


def _is_forbidden_callable(name: str) -> bool:
    leaf = name.rsplit(".", 1)[-1]
    tokens = _recorded_tokens(leaf)
    if "submit" in tokens or "transmit" in tokens:
        return True
    return "place" in tokens and "order" in tokens


def _target_is_forbidden(target: str) -> bool:
    return _is_forbidden_callable(target)


def _is_protocol_name(target: str) -> bool:
    return target in {"Protocol", "typing.Protocol", "typing_extensions.Protocol"} or target.endswith(".Protocol")


def _resolve_import_module(
    module: _ModuleInfo, imported: str | None, level: int
) -> str:
    imported_parts = [] if not imported else imported.split(".")
    if level == 0:
        return ".".join(imported_parts)
    anchor = module.module.split(".")
    if not module.is_package:
        anchor = anchor[:-1]
    remove = level - 1
    if remove > len(anchor):
        return ".".join(imported_parts)
    if remove:
        anchor = anchor[:-remove]
    return ".".join((*anchor, *imported_parts))


def _qualified_node_symbol(module: _ModuleInfo, node: ast.AST) -> str:
    names: list[str] = []
    current: ast.AST | None = node
    while current is not None:
        if isinstance(current, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            names.append(current.name)
        current = module.parents.get(current)
    names.reverse()
    return ".".join((module.module, *names))


def _nearest_parent(
    module: _ModuleInfo, node: ast.AST, node_type: type[ast.AST]
) -> ast.AST | None:
    current = module.parents.get(node)
    while current is not None:
        if isinstance(current, node_type):
            return current
        current = module.parents.get(current)
    return None


def _module_name(root: Path, path: Path) -> str:
    relative = path.relative_to(root)
    parts = list(relative.parts)
    if parts[-1] == "__init__.py":
        parts.pop()
    else:
        parts[-1] = Path(parts[-1]).stem
    return ".".join((root.name, *parts))


def _assigned_names(target: ast.expr) -> Iterable[str]:
    if isinstance(target, ast.Name):
        yield target.id
    elif isinstance(target, (ast.Tuple, ast.List)):
        for item in target.elts:
            yield from _assigned_names(item)


def _deduplicate_violations(
    violations: Iterable[AuditViolation],
) -> tuple[AuditViolation, ...]:
    unique: dict[tuple[object, ...], AuditViolation] = {}
    for violation in violations:
        key = (
            violation.path,
            violation.line,
            violation.column,
            violation.kind,
            violation.symbol,
            violation.target,
            violation.trace,
        )
        unique[key] = violation
    return tuple(
        sorted(
            unique.values(),
            key=lambda item: (
                item.path,
                item.line,
                item.column,
                item.kind,
                item.symbol,
                item.target,
            ),
        )
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit Options Copilot for broker write authority"
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("options_copilot"),
        help="production package root to scan recursively",
    )
    parser.add_argument("--json", action="store_true", help="emit JSON report")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _build_parser().parse_args(argv)
    try:
        report = audit_repository(arguments.root)
    except (OSError, ValueError) as exc:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "ok": False,
            "error": str(exc),
        }
        if arguments.json:
            print(json.dumps(payload, indent=2, ensure_ascii=False))
        else:
            print(f"authority audit failed: {exc}")
        return 2
    if arguments.json:
        print(json.dumps(report.as_dict(), indent=2, ensure_ascii=False))
    elif report.ok:
        print(
            f"authority audit passed: {report.files_scanned} files, "
            f"{report.callables_scanned} callables"
        )
    else:
        for violation in report.violations:
            print(
                f"{violation.path}:{violation.line}:{violation.column}: "
                f"{violation.kind}: {violation.symbol} -> {violation.target}"
            )
    return 0 if report.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "AuditViolation",
    "AuthorityAuditReport",
    "audit",
    "audit_repository",
    "main",
]
