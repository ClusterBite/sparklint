//! Source loading for sparklint.
//!
//! Three input kinds are supported:
//! - plain Python modules (`.py`),
//! - Databricks *source-format* notebooks (`.py` files starting with
//!   `# Databricks notebook source`, cells separated by `# COMMAND ----------`,
//!   non-Python cells encoded as `# MAGIC %sql` … comment lines),
//! - Jupyter notebooks (`.ipynb`, via `ruff_notebook`: code cells are concatenated
//!   into one Python source and a cell map is kept).
//!
//! Every [`SourceFile`] exposes the Python text that will be parsed, a cell map
//! (empty for plain modules) and a line index so diagnostics can be reported as
//! `line:col` or `cell N, line:col`.
//!
//! Databricks non-Python cells consist only of `# MAGIC` comment lines, so the Python
//! parser never sees their content as code; their text is still available through
//! [`Cell::magic_body`] (e.g. for SQL checks).
//!
//! # Magics
//!
//! Real notebooks also contain *bare* magics: `%pip install x`, `!pip install x`,
//! `%run ./setup` in hand-edited Databricks source files, and `%sql` / `%scala` / `%sh`
//! as the first line of a (Databricks-exported) `.ipynb` cell. Two mechanisms keep them
//! out of the Python syntax:
//!
//! - **Line magics and shell escapes** are left in the text. Both notebook kinds are
//!   parsed in IPython mode ([`SourceFile::py_source_type`]), where the lexer turns a
//!   `%…` / `!…` line into one escape-command token. Only the lexer knows whether a
//!   `%` starts a logical line (and not a continuation line or a line of a string), it
//!   handles backslash continuations and `x = !cmd`, and the text stays byte-identical,
//!   so fixes write the magics back untouched.
//! - **Language cells** (a cell whose first line is `%sql`, `%scala`, `%r`, `%md`,
//!   `%sh`, `%bash`, `%fs`: the whole cell is another language) are blanked in
//!   [`SourceFile::text`]: every byte except line breaks becomes a space, so offsets and
//!   line numbers are unchanged. The untouched text stays available through
//!   [`SourceFile::raw_text`] (what fixes are applied to and what is shown to the user).

use std::path::{Path, PathBuf};

use ruff_notebook::Notebook;
use ruff_python_ast::PySourceType;
use ruff_text_size::{TextRange, TextSize};

/// Header line that marks a Databricks source-format notebook.
pub const DATABRICKS_HEADER: &str = "# Databricks notebook source";
/// Cell separator line in a Databricks source-format notebook.
pub const DATABRICKS_CELL_SEP: &str = "# COMMAND ----------";

#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error("failed to read {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to load notebook {path}: {message}")]
    Notebook { path: PathBuf, message: String },
}

/// What kind of file a source came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SourceKind {
    /// A regular Python module.
    Python,
    /// A Databricks source-format notebook (`# Databricks notebook source`).
    DatabricksNotebook,
    /// A Jupyter notebook (`.ipynb`).
    Jupyter,
}

impl SourceKind {
    pub fn is_notebook(self) -> bool {
        !matches!(self, SourceKind::Python)
    }
}

/// The language of a notebook cell.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CellKind {
    Python,
    Sql,
    Markdown,
    /// `%run ./other_notebook`
    Run,
    /// `%pip install …`
    Pip,
    /// `%sh`
    Shell,
    /// `%fs`
    Fs,
    Scala,
    R,
    /// `%python` written as `# MAGIC` lines (not parsed as Python).
    MagicPython,
    /// Any other magic (`%lsmagic`, `%config`, …); the payload is the magic name.
    Other(String),
}

impl CellKind {
    fn from_magic(name: &str) -> Self {
        match name {
            "sql" => CellKind::Sql,
            "md" | "md-sandbox" | "markdown" => CellKind::Markdown,
            "run" => CellKind::Run,
            "pip" | "conda" => CellKind::Pip,
            "sh" | "bash" => CellKind::Shell,
            "fs" => CellKind::Fs,
            "scala" => CellKind::Scala,
            "r" => CellKind::R,
            "python" => CellKind::MagicPython,
            other => CellKind::Other(other.to_string()),
        }
    }

    pub fn is_python(&self) -> bool {
        matches!(self, CellKind::Python)
    }
}

/// Magics that turn the whole cell into another language (Databricks `%sql`, `%sh`, …).
/// A cell starting with one of these is not Python at all.
pub const LANGUAGE_MAGICS: &[&str] = &[
    "sql",
    "scala",
    "r",
    "md",
    "md-sandbox",
    "markdown",
    "sh",
    "bash",
    "fs",
];

/// `%name rest` at the start of a line → `(name, rest)`. `None` for `%%name`, for a
/// lone `%` and for `%name(` (the `%` operator on a continuation line).
pub fn line_magic(line: &str) -> Option<(&str, &str)> {
    let rest = line.trim_start().strip_prefix('%')?;
    let end = rest
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '-'))
        .unwrap_or(rest.len());
    let (name, tail) = rest.split_at(end);
    if name.is_empty() || !(tail.is_empty() || tail.starts_with(char::is_whitespace)) {
        return None;
    }
    Some((name, tail.trim()))
}

/// A `%run` of another notebook (`# MAGIC %run ./setup` or a bare `%run ./setup`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunTarget {
    /// 1-based number of the cell containing the `%run`.
    pub cell: u32,
    /// 1-based line in [`SourceFile::text`].
    pub line: u32,
    /// The notebook path as written (quotes removed), without arguments.
    pub path: String,
}

/// One notebook cell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cell {
    /// 1-based cell number as shown in the notebook UI (for `.ipynb`, the raw cell
    /// index including markdown cells).
    pub number: u32,
    /// Range of the cell inside [`SourceFile::text`].
    pub range: TextRange,
    /// 1-based line (in the full text) on which the cell's content starts.
    pub first_line: u32,
    pub kind: CellKind,
    /// For magic cells: the cell body without the `# MAGIC ` prefixes and without the
    /// magic keyword line (e.g. the SQL text of a `%sql` cell).
    pub magic_body: Option<String>,
}

/// A resolved position for reporting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Location {
    /// 1-based line in the (concatenated) source text.
    pub line: u32,
    /// 1-based column, in characters.
    pub column: u32,
    /// 1-based notebook cell number, if the file is a notebook.
    pub cell: Option<u32>,
    /// 1-based line inside the cell, if the file is a notebook.
    pub cell_line: Option<u32>,
}

/// Simple line index over a text (line starts as byte offsets).
#[derive(Debug, Clone, Default)]
pub struct LineIndex {
    starts: Vec<TextSize>,
}

impl LineIndex {
    pub fn new(text: &str) -> Self {
        let mut starts = vec![TextSize::from(0)];
        for (i, b) in text.bytes().enumerate() {
            if b == b'\n' {
                starts.push(TextSize::from((i + 1) as u32));
            }
        }
        Self { starts }
    }

    /// 1-based line number containing `offset`.
    pub fn line_of(&self, offset: TextSize) -> u32 {
        match self.starts.binary_search(&offset) {
            Ok(i) => i as u32 + 1,
            Err(i) => i as u32,
        }
    }

    /// Byte offset of the start of the given 1-based line (clamped).
    pub fn line_start(&self, line: u32) -> TextSize {
        let idx = (line.max(1) - 1) as usize;
        self.starts
            .get(idx)
            .copied()
            .unwrap_or_else(|| *self.starts.last().unwrap_or(&TextSize::from(0)))
    }

    pub fn line_count(&self) -> u32 {
        self.starts.len() as u32
    }

    /// The text of a 1-based line without its line terminator.
    pub fn line_text<'t>(&self, text: &'t str, line: u32) -> &'t str {
        let start = usize::from(self.line_start(line)).min(text.len());
        let end = if (line as usize) < self.starts.len() {
            usize::from(self.starts[line as usize])
        } else {
            text.len()
        };
        let end = end.max(start).min(text.len());
        text[start..end].trim_end_matches(['\n', '\r'])
    }

    /// 1-based (line, column) with the column counted in characters.
    pub fn line_col(&self, text: &str, offset: TextSize) -> (u32, u32) {
        let line = self.line_of(offset);
        let start = usize::from(self.line_start(line)).min(text.len());
        let off = usize::from(offset).min(text.len()).max(start);
        let col = text
            .get(start..off)
            .map(|s| s.chars().count())
            .unwrap_or(off - start);
        (line, col as u32 + 1)
    }
}

/// A loaded source file ready to be parsed.
#[derive(Debug, Clone)]
pub struct SourceFile {
    pub path: PathBuf,
    /// The Python text that is parsed (for `.ipynb`: concatenated code cells).
    pub text: String,
    pub kind: SourceKind,
    /// Cell map; empty for plain Python modules.
    pub cells: Vec<Cell>,
    pub line_index: LineIndex,
    /// The text before language cells were blanked (`None`: nothing was blanked).
    /// Same length and line breaks as `text`.
    raw: Option<String>,
    /// A Databricks notebook: source format, or `.ipynb` with Databricks metadata.
    databricks: bool,
}

impl SourceFile {
    /// Load a file from disk, detecting its kind from extension and header.
    pub fn load(path: &Path) -> Result<Self, SourceError> {
        let is_ipynb = path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("ipynb"));
        let bytes = std::fs::read(path).map_err(|source| SourceError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let text = String::from_utf8_lossy(&bytes).into_owned();
        if is_ipynb {
            return Self::from_ipynb_json(path, &text);
        }
        Ok(Self::from_text(path, text))
    }

    /// Build from in-memory text; detects Databricks source notebooks by header.
    pub fn from_text(path: &Path, text: String) -> Self {
        let line_index = LineIndex::new(&text);
        let is_dbx = text
            .trim_start_matches('\u{feff}')
            .lines()
            .next()
            .is_some_and(|l| l.trim_end() == DATABRICKS_HEADER);
        if !is_dbx {
            return Self {
                path: path.to_path_buf(),
                text,
                kind: SourceKind::Python,
                cells: Vec::new(),
                line_index,
                raw: None,
                databricks: false,
            };
        }
        let (cells, language_cells) = databricks_cells(&text);
        let (text, raw) = blank_cells(text, &language_cells);
        Self {
            path: path.to_path_buf(),
            text,
            kind: SourceKind::DatabricksNotebook,
            cells,
            line_index,
            raw,
            databricks: true,
        }
    }

    /// Build from an in-memory `.ipynb` JSON string.
    pub fn from_ipynb_json(path: &Path, json: &str) -> Result<Self, SourceError> {
        let notebook = Notebook::from_source_code(json).map_err(|e| SourceError::Notebook {
            path: path.to_path_buf(),
            message: e.to_string(),
        })?;
        // Databricks exports mark the notebook and every cell with its own MIME key.
        let databricks = json.contains(DATABRICKS_NOTEBOOK_KEY)
            || notebook
                .cells()
                .iter()
                .any(|c| c.metadata().extra.contains_key(DATABRICKS_CELL_KEY));
        Ok(Self::from_notebook(path, &notebook, databricks))
    }

    fn from_notebook(path: &Path, notebook: &Notebook, databricks: bool) -> Self {
        let text = notebook.source_code().to_string();
        let line_index = LineIndex::new(&text);
        let starts: Vec<_> = notebook.index().iter().collect();
        let mut cells = Vec::new();
        let mut language_cells = Vec::new();
        for (i, range) in notebook.cell_offsets().ranges().enumerate() {
            let number = starts
                .get(i)
                .map(|s| s.cell_index().get() as u32)
                .unwrap_or(i as u32 + 1);
            let body = &text[usize::from(range.start()).min(text.len())
                ..usize::from(range.end()).min(text.len())];
            let first = body.lines().find(|l| !l.trim().is_empty()).unwrap_or("");
            let (kind, magic_body) = if let Some(rest) = first.trim_start().strip_prefix("%%") {
                let name = rest.split_whitespace().next().unwrap_or("").to_lowercase();
                let after: String = body
                    .lines()
                    .skip_while(|l| l.trim().is_empty())
                    .skip(1)
                    .collect::<Vec<_>>()
                    .join("\n");
                (CellKind::from_magic(&name), Some(after))
            } else if let Some(bare) = classify_bare_cell(body) {
                if bare.language {
                    language_cells.push(range);
                }
                (bare.kind, bare.body)
            } else {
                (CellKind::Python, None)
            };
            cells.push(Cell {
                number,
                range,
                first_line: line_index.line_of(range.start()),
                kind,
                magic_body,
            });
        }
        let (text, raw) = blank_cells(text, &language_cells);
        Self {
            path: path.to_path_buf(),
            text,
            kind: SourceKind::Jupyter,
            cells,
            line_index,
            raw,
            databricks,
        }
    }

    pub fn is_notebook(&self) -> bool {
        self.kind.is_notebook()
    }

    /// A Databricks notebook: source format (`# Databricks notebook source`), or an
    /// `.ipynb` carrying Databricks metadata. Its session (`spark`) already runs.
    pub fn is_databricks(&self) -> bool {
        self.databricks
    }

    /// The text as written, before language cells were blanked in [`Self::text`]
    /// (same length, same line breaks). Fixes are applied to this text.
    pub fn raw_text(&self) -> &str {
        self.raw.as_deref().unwrap_or(&self.text)
    }

    /// Source type for the parser. Both notebook kinds are parsed in IPython mode, so
    /// `%magic` / `!shell` lines are escape commands instead of syntax errors.
    pub fn py_source_type(&self) -> PySourceType {
        match self.kind {
            SourceKind::Jupyter | SourceKind::DatabricksNotebook => PySourceType::Ipynb,
            SourceKind::Python => PySourceType::Python,
        }
    }

    /// The index (into [`Self::cells`]) of the cell containing `offset`.
    pub fn cell_index_at(&self, offset: TextSize) -> Option<usize> {
        self.cells
            .partition_point(|c| c.range.start() <= offset)
            .checked_sub(1)
    }

    /// Every `%run` of another notebook: `# MAGIC %run ./x` lines and bare `%run ./x`
    /// lines (Databricks source and `.ipynb`), in file order.
    pub fn run_targets(&self) -> Vec<RunTarget> {
        let raw = self.raw_text();
        let mut out = Vec::new();
        for cell in &self.cells {
            // A `%run` inside a `%sh` / `%sql` / `%md` cell is that language's text.
            if !matches!(
                cell.kind,
                CellKind::Python | CellKind::Run | CellKind::Pip | CellKind::Other(_)
            ) {
                continue;
            }
            let body = raw
                .get(usize::from(cell.range.start())..usize::from(cell.range.end()))
                .unwrap_or("");
            for (i, line) in body.lines().enumerate() {
                let t = line.trim_start();
                let t = t.strip_prefix("# MAGIC").map_or(t, str::trim_start);
                if let Some((name, rest)) = line_magic(t)
                    && name == "run"
                    && let Some(path) = run_path(rest)
                {
                    out.push(RunTarget {
                        cell: cell.number,
                        line: cell.first_line + i as u32,
                        path,
                    });
                }
            }
        }
        out
    }

    /// The cell containing `offset`, if this is a notebook.
    pub fn cell_at(&self, offset: TextSize) -> Option<&Cell> {
        self.cells.get(self.cell_index_at(offset)?)
    }

    /// Resolve an offset into a reportable location.
    pub fn location(&self, offset: TextSize) -> Location {
        let (line, column) = self.line_index.line_col(&self.text, offset);
        let (cell, cell_line) = match self.cell_at(offset) {
            Some(c) => (Some(c.number), Some(line.saturating_sub(c.first_line) + 1)),
            None => (None, None),
        };
        Location {
            line,
            column,
            cell,
            cell_line,
        }
    }

    /// The text of a 1-based line as written (also for lines of a blanked cell).
    pub fn line_text(&self, line: u32) -> &str {
        self.line_index.line_text(self.raw_text(), line)
    }

    pub fn slice(&self, range: TextRange) -> &str {
        let start = usize::from(range.start()).min(self.text.len());
        let end = usize::from(range.end()).min(self.text.len()).max(start);
        self.text.get(start..end).unwrap_or("")
    }
}

const DATABRICKS_NOTEBOOK_KEY: &str = "application/vnd.databricks.v1+notebook";
const DATABRICKS_CELL_KEY: &str = "application/vnd.databricks.v1+cell";

/// The notebook path of a `%run` line: the first argument, quotes removed.
fn run_path(rest: &str) -> Option<String> {
    let rest = rest.trim();
    let path = match rest.chars().next()? {
        q @ ('"' | '\'') => rest[1..].split(q).next().unwrap_or(""),
        _ => rest.split_whitespace().next().unwrap_or(""),
    };
    (!path.is_empty()).then(|| path.to_string())
}

/// Blank `ranges` of `text` (every byte except line breaks becomes a space): the
/// result has the same length and line starts. Returns `(blanked, Some(original))`,
/// or `(text, None)` when there is nothing to blank.
fn blank_cells(text: String, ranges: &[TextRange]) -> (String, Option<String>) {
    if ranges.is_empty() {
        return (text, None);
    }
    let mut bytes = text.clone().into_bytes();
    for r in ranges {
        blank_range(&mut bytes, *r);
    }
    // Cell ranges start and end at line starts, so whole characters were replaced.
    match String::from_utf8(bytes) {
        Ok(blanked) => (blanked, Some(text)),
        Err(_) => (text, None),
    }
}

/// Replace every byte of `range` except `\n` / `\r` with a space.
pub fn blank_range(bytes: &mut [u8], range: TextRange) {
    let end = usize::from(range.end()).min(bytes.len());
    let start = usize::from(range.start()).min(end);
    for b in &mut bytes[start..end] {
        if !matches!(*b, b'\n' | b'\r') {
            *b = b' ';
        }
    }
}

/// A cell that starts with a bare magic (`%sql`, `%pip install x`, `!ls`).
struct BareCell {
    kind: CellKind,
    body: Option<String>,
    /// The whole cell is another language and must not be parsed as Python.
    language: bool,
}

/// Classify a cell whose first line is a bare `%magic` / `!shell` line.
///
/// - A language magic ([`LANGUAGE_MAGICS`]) makes the whole cell that language.
/// - Any other magic (`%run`, `%pip`, `%restart_python`, …) names the cell's kind only
///   when the cell holds no Python code besides magics; a cell mixing magics and
///   Python stays a Python cell (the magic lines are escape commands for the parser).
fn classify_bare_cell(body: &str) -> Option<BareCell> {
    // `# DBTITLE 1,Title` is cell metadata written before the cell's first line.
    let content = |l: &&str| !l.trim().is_empty() && !l.trim_start().starts_with("# DBTITLE");
    // Logical lines: a line ending in a backslash continues on the next one.
    let mut continued = false;
    let mut lines = body.lines().filter(|l| {
        let continuation = continued;
        continued = l.trim_end().ends_with('\\');
        !continuation && content(l)
    });
    let first = lines.next()?.trim_start();
    let rest: Vec<&str> = lines.collect();
    let only_magics = rest.iter().all(|l| {
        let t = l.trim_start();
        t.starts_with('#') || t.starts_with('%') || t.starts_with('!')
    });
    if let Some(cmd) = first.strip_prefix('!') {
        if !only_magics {
            return None;
        }
        let mut out = vec![cmd.trim()];
        out.extend(rest.iter().filter_map(|l| l.trim_start().strip_prefix('!')));
        return Some(BareCell {
            kind: CellKind::Shell,
            body: Some(out.join("\n")),
            language: false,
        });
    }
    // `%%sql` in a Databricks source file: same as `%sql`.
    let single = if first.starts_with("%%") {
        &first[1..]
    } else {
        first
    };
    let (name, args) = line_magic(single)?;
    let name = name.to_lowercase();
    if LANGUAGE_MAGICS.contains(&name.as_str()) {
        // The cell's text in its own language: everything after the magic keyword.
        let mut out = Vec::new();
        if !args.is_empty() {
            out.push(args);
        }
        out.extend(body.lines().skip_while(|l| !content(l)).skip(1));
        return Some(BareCell {
            kind: CellKind::from_magic(&name),
            body: Some(out.join("\n").trim_end().to_string()),
            language: true,
        });
    }
    if !only_magics || name == "python" {
        return None;
    }
    Some(BareCell {
        kind: CellKind::from_magic(&name),
        body: Some(args.to_string()),
        language: false,
    })
}

/// Cells of a Databricks source notebook, plus the ranges of the bare language cells
/// (to be blanked in the Python view).
fn databricks_cells(text: &str) -> (Vec<Cell>, Vec<TextRange>) {
    let mut cells = Vec::new();
    let mut language_cells = Vec::new();
    let mut offset = 0usize;
    // (start offset of content, first line)
    let mut cur_start: Option<(usize, u32)> = None;
    let mut line_no = 0u32;
    let mut push = |start: usize, end: usize, first_line: u32, cells: &mut Vec<Cell>| {
        let number = cells.len() as u32 + 1;
        let body = &text[start..end];
        let range = TextRange::new(TextSize::from(start as u32), TextSize::from(end as u32));
        let (kind, magic_body) = match classify_bare_cell(body) {
            Some(bare) => {
                if bare.language {
                    language_cells.push(range);
                }
                (bare.kind, bare.body)
            }
            None => classify_dbx_cell(body),
        };
        cells.push(Cell {
            number,
            range,
            first_line,
            kind,
            magic_body,
        });
    };
    for line in text.split_inclusive('\n') {
        line_no += 1;
        let trimmed = line.trim_end();
        let line_start = offset;
        offset += line.len();
        if line_no == 1 && trimmed.trim_start_matches('\u{feff}') == DATABRICKS_HEADER {
            continue;
        }
        if trimmed == DATABRICKS_CELL_SEP {
            if let Some((s, fl)) = cur_start.take() {
                push(s, line_start, fl, &mut cells);
            } else if line_no > 2 {
                // empty cell between separators
                push(line_start, line_start, line_no, &mut cells);
            }
            continue;
        }
        if cur_start.is_none() {
            cur_start = Some((line_start, line_no));
        }
    }
    if let Some((s, fl)) = cur_start.take() {
        push(s, text.len(), fl, &mut cells);
    }
    (cells, language_cells)
}

fn classify_dbx_cell(body: &str) -> (CellKind, Option<String>) {
    // `# DBTITLE 1,Title` is cell metadata written before the cell's first line.
    let content = |l: &&str| !l.trim().is_empty() && !l.trim_start().starts_with("# DBTITLE");
    let mut lines = body.lines().filter(content);
    let Some(first) = lines.next() else {
        return (CellKind::Python, None);
    };
    let Some(rest) = first.trim_start().strip_prefix("# MAGIC") else {
        return (CellKind::Python, None);
    };
    let rest = rest.trim_start();
    let Some(magic) = rest.strip_prefix('%') else {
        return (CellKind::Python, None);
    };
    let name = magic.split_whitespace().next().unwrap_or("").to_lowercase();
    let first_line_rest = magic[name.len().min(magic.len())..].trim().to_string();
    let mut out = Vec::new();
    if !first_line_rest.is_empty() {
        out.push(first_line_rest);
    }
    for l in body.lines().skip_while(|l| !content(l)).skip(1) {
        let t = l.trim_start();
        if let Some(r) = t.strip_prefix("# MAGIC") {
            out.push(r.strip_prefix(' ').unwrap_or(r).to_string());
        }
    }
    (CellKind::from_magic(&name), Some(out.join("\n")))
}

#[cfg(test)]
mod tests {
    use super::*;

    const DBX: &str = "# Databricks notebook source\nimport x\n\n# COMMAND ----------\n\n# MAGIC %sql\n# MAGIC SELECT * FROM t\n\n# COMMAND ----------\n\ndf = spark.table('t')\ndf.collect()\n";

    #[test]
    fn plain_python_has_no_cells() {
        let f = SourceFile::from_text(Path::new("a.py"), "x = 1\n".into());
        assert_eq!(f.kind, SourceKind::Python);
        assert!(f.cells.is_empty());
        assert!(!f.is_notebook());
        let loc = f.location(TextSize::from(4));
        assert_eq!((loc.line, loc.column, loc.cell), (1, 5, None));
    }

    #[test]
    fn databricks_cells_are_split() {
        let f = SourceFile::from_text(Path::new("nb.py"), DBX.into());
        assert_eq!(f.kind, SourceKind::DatabricksNotebook);
        assert_eq!(f.cells.len(), 3);
        assert_eq!(f.cells[0].kind, CellKind::Python);
        assert_eq!(f.cells[1].kind, CellKind::Sql);
        assert_eq!(f.cells[1].magic_body.as_deref(), Some("SELECT * FROM t"));
        assert_eq!(f.cells[2].kind, CellKind::Python);
        let off = DBX.find("df.collect").unwrap();
        let loc = f.location(TextSize::from(off as u32));
        assert_eq!(loc.line, 12);
        assert_eq!(loc.cell, Some(3));
        // cell 3 content starts at line 10 (blank line after the separator)
        assert_eq!(loc.cell_line, Some(3));
    }

    #[test]
    fn magic_run_and_pip() {
        let src = "# Databricks notebook source\n# MAGIC %run ./setup\n\n# COMMAND ----------\n\n# MAGIC %pip install foo\n";
        let f = SourceFile::from_text(Path::new("nb.py"), src.into());
        assert_eq!(f.cells.len(), 2);
        assert_eq!(f.cells[0].kind, CellKind::Run);
        assert_eq!(f.cells[0].magic_body.as_deref(), Some("./setup"));
        assert_eq!(f.cells[1].kind, CellKind::Pip);
    }

    #[test]
    fn ipynb_cells_and_locations() {
        let json = r##"{
 "cells": [
  {"cell_type": "markdown", "metadata": {}, "source": ["# Title"]},
  {"cell_type": "code", "execution_count": null, "metadata": {}, "outputs": [], "source": ["import x\n", "y = 1"]},
  {"cell_type": "code", "execution_count": null, "metadata": {}, "outputs": [], "source": ["%%sql\n", "SELECT 1"]},
  {"cell_type": "code", "execution_count": null, "metadata": {}, "outputs": [], "source": ["%pip install foo\n", "df.collect()"]}
 ],
 "metadata": {},
 "nbformat": 4,
 "nbformat_minor": 5
}"##;
        let f = SourceFile::from_ipynb_json(Path::new("a.ipynb"), json).unwrap();
        assert_eq!(f.kind, SourceKind::Jupyter);
        // ruff_notebook drops cells with non-Python cell magics (`%%sql`)
        assert_eq!(f.cells.len(), 2);
        assert_eq!(f.cells[0].number, 2);
        assert_eq!(f.cells[1].number, 4);
        assert!(!f.text.contains("SELECT 1"));
        let off = f.text.find("df.collect").unwrap();
        let loc = f.location(TextSize::from(off as u32));
        assert_eq!(loc.cell, Some(4));
        assert_eq!(loc.cell_line, Some(2));
        assert_eq!(f.py_source_type(), PySourceType::Ipynb);
    }

    /// A hand-edited Databricks source notebook: bare magics next to `# MAGIC` cells.
    const BARE: &str = "# Databricks notebook source\n%pip install --quiet faker \\\n    colorama\n%restart_python\n\n# COMMAND ----------\n\n!pip install confluent-kafka\n\n# COMMAND ----------\n\n%run ./_resources/00-setup $reset_all_data=false\n\n# COMMAND ----------\n\n# DBTITLE 1,Shell\n%sh\nls -l /dbfs/tmp | wc -l\nif [ -d /tmp/é ]; then rm -rf /tmp/x; fi\n\n# COMMAND ----------\n\n%sql\nSELECT 'it''s' FROM t\n\n# COMMAND ----------\n\n# MAGIC %run \"./other notebook\" $x=\"1\"\n\n# COMMAND ----------\n\ndf = spark.table('t')\nfiles = !ls /tmp\n%run ./inline\nrows = df.collect()\n";

    #[test]
    fn bare_magic_cells_are_classified() {
        let f = SourceFile::from_text(Path::new("nb.py"), BARE.into());
        assert!(f.is_databricks());
        let kinds: Vec<&CellKind> = f.cells.iter().map(|c| &c.kind).collect();
        assert_eq!(
            kinds,
            [
                &CellKind::Pip,
                &CellKind::Shell,
                &CellKind::Run,
                &CellKind::Shell,
                &CellKind::Sql,
                &CellKind::Run,
                &CellKind::Python,
            ]
        );
        assert_eq!(
            f.cells[1].magic_body.as_deref(),
            Some("pip install confluent-kafka")
        );
        assert_eq!(
            f.cells[2].magic_body.as_deref(),
            Some("./_resources/00-setup $reset_all_data=false")
        );
        assert_eq!(
            f.cells[3].magic_body.as_deref(),
            Some("ls -l /dbfs/tmp | wc -l\nif [ -d /tmp/é ]; then rm -rf /tmp/x; fi")
        );
        assert_eq!(
            f.cells[4].magic_body.as_deref(),
            Some("SELECT 'it''s' FROM t")
        );
        // `# DBTITLE` before a `# MAGIC` cell does not hide its kind either.
        let f = SourceFile::from_text(
            Path::new("nb.py"),
            "# Databricks notebook source\n# DBTITLE 1,Query\n# MAGIC %sql\n# MAGIC SELECT 1\n"
                .into(),
        );
        assert_eq!(f.cells[0].kind, CellKind::Sql);
        assert_eq!(f.cells[0].magic_body.as_deref(), Some("SELECT 1"));
    }

    #[test]
    fn language_cells_are_blanked_with_stable_offsets() {
        let f = SourceFile::from_text(Path::new("nb.py"), BARE.into());
        // Same length, same line starts: every offset and line number is unchanged.
        assert_eq!(f.raw_text(), BARE);
        assert_eq!(f.text.len(), BARE.len());
        let breaks = |t: &str| -> Vec<usize> { t.match_indices('\n').map(|(i, _)| i).collect() };
        assert_eq!(breaks(&f.text), breaks(BARE));
        // The `%sh` and `%sql` cells are gone from the Python view …
        assert!(!f.text.contains("ls -l") && !f.text.contains("rm -rf"));
        assert!(!f.text.contains("%sh") && !f.text.contains("SELECT"));
        // … the `# DBTITLE` line of the blanked cell too (it is part of the cell) …
        assert!(!f.text.contains("DBTITLE"));
        // … and nothing else changed: line magics, shell escapes, separators, Python.
        for keep in [
            "%pip install --quiet faker \\\n    colorama\n%restart_python\n",
            "!pip install confluent-kafka\n",
            "%run ./_resources/00-setup $reset_all_data=false\n",
            "# MAGIC %run \"./other notebook\" $x=\"1\"\n",
            "files = !ls /tmp\n%run ./inline\nrows = df.collect()\n",
        ] {
            assert_eq!(f.text.find(keep), BARE.find(keep), "{keep}");
        }
        assert_eq!(f.text.matches(DATABRICKS_CELL_SEP).count(), 6);
        // Locations resolve as before, and a blanked line still shows its text.
        let off = BARE.find("rows = df.collect").unwrap();
        let loc = f.location(TextSize::from(off as u32));
        assert_eq!((loc.line, loc.cell, loc.cell_line), (35, Some(7), Some(5)));
        assert_eq!(f.line_text(18), "ls -l /dbfs/tmp | wc -l");
        assert_eq!(f.text.lines().nth(17).map(str::trim), Some(""));
        // Both notebook kinds are lexed in IPython mode.
        assert_eq!(f.py_source_type(), PySourceType::Ipynb);
        // A notebook without language cells keeps one copy of the text.
        let plain = SourceFile::from_text(Path::new("nb.py"), DBX.into());
        assert_eq!(plain.raw_text().as_ptr(), plain.text.as_ptr());
    }

    #[test]
    fn run_targets_bare_and_magic() {
        let f = SourceFile::from_text(Path::new("nb.py"), BARE.into());
        let targets = f.run_targets();
        let runs: Vec<(u32, u32, &str)> = targets
            .iter()
            .map(|r| (r.cell, r.line, r.path.as_str()))
            .collect();
        assert_eq!(
            runs,
            [
                (3, 12, "./_resources/00-setup"),
                (6, 28, "./other notebook"),
                (7, 34, "./inline"),
            ]
        );
        // `%run` inside a shell cell is shell text.
        let f = SourceFile::from_text(
            Path::new("nb.py"),
            "# Databricks notebook source\n%sh\n%run ./not-a-notebook\n".into(),
        );
        assert!(f.run_targets().is_empty());
    }

    #[test]
    fn line_magic_names() {
        assert_eq!(line_magic("%pip install x"), Some(("pip", "install x")));
        assert_eq!(line_magic("  %md-sandbox"), Some(("md-sandbox", "")));
        assert_eq!(line_magic("%restart_python"), Some(("restart_python", "")));
        // `%%cell` magics, the `%` operator on a continuation line, a lone `%`.
        assert_eq!(line_magic("%%sql"), None);
        assert_eq!(line_magic("% 7)"), None);
        assert_eq!(line_magic("%(name)s"), None);
        assert_eq!(line_magic("x = 1"), None);
    }

    fn ipynb(cells: &[&str], cell_meta: &str, nb_meta: &str) -> String {
        let cells: Vec<String> = cells
            .iter()
            .map(|src| {
                let src = src.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n");
                format!(
                    r#"{{"cell_type": "code", "execution_count": null, "metadata": {{{cell_meta}}}, "outputs": [], "source": "{src}"}}"#
                )
            })
            .collect();
        format!(
            r#"{{"cells": [{}], "metadata": {{{nb_meta}}}, "nbformat": 4, "nbformat_minor": 2}}"#,
            cells.join(", ")
        )
    }

    #[test]
    fn ipynb_language_cells_are_blanked() {
        // A Databricks export marks language cells with a single `%`.
        let json = ipynb(
            &[
                "df = spark.table('t')",
                "%sql\nSELECT * FROM t WHERE x = 'it''s'",
                "%scala\nval x = 1",
                "%pip install faker\nrows = df.collect()",
                "%run ./setup",
                "%md\n# title",
            ],
            r#""application/vnd.databricks.v1+cell": {"title": ""}"#,
            r#""application/vnd.databricks.v1+notebook": {"language": "python"}"#,
        );
        let f = SourceFile::from_ipynb_json(Path::new("a.ipynb"), &json).unwrap();
        assert!(f.is_databricks());
        let kinds: Vec<&CellKind> = f.cells.iter().map(|c| &c.kind).collect();
        assert_eq!(
            kinds,
            [
                &CellKind::Python,
                &CellKind::Sql,
                &CellKind::Scala,
                &CellKind::Python,
                &CellKind::Run,
                &CellKind::Markdown,
            ]
        );
        assert_eq!(
            f.cells[1].magic_body.as_deref(),
            Some("SELECT * FROM t WHERE x = 'it''s'")
        );
        assert!(
            !f.text.contains("SELECT") && !f.text.contains("val x") && !f.text.contains("title")
        );
        assert!(f.text.contains("%pip install faker\nrows = df.collect()\n"));
        assert!(f.text.contains("%run ./setup\n"));
        assert_eq!(f.text.len(), f.raw_text().len());
        assert!(f.raw_text().contains("SELECT * FROM t"));
        let off = f.text.find("rows = df.collect").unwrap();
        let loc = f.location(TextSize::from(off as u32));
        assert_eq!((loc.cell, loc.cell_line), (Some(4), Some(2)));
        assert_eq!(f.run_targets().len(), 1);
        assert_eq!(f.run_targets()[0].path, "./setup");
        assert_eq!(f.run_targets()[0].cell, 5);
    }

    #[test]
    fn databricks_ipynb_is_detected_by_metadata() {
        let plain = ipynb(
            &["x = 1"],
            "",
            r#""kernelspec": {"name": "python3", "language": "python"}"#,
        );
        let f = SourceFile::from_ipynb_json(Path::new("a.ipynb"), &plain).unwrap();
        assert!(!f.is_databricks());
        let by_cell = ipynb(
            &["x = 1"],
            r#""application/vnd.databricks.v1+cell": {}"#,
            "",
        );
        let f = SourceFile::from_ipynb_json(Path::new("a.ipynb"), &by_cell).unwrap();
        assert!(f.is_databricks());
        let by_notebook = ipynb(
            &["x = 1"],
            "",
            r#""application/vnd.databricks.v1+notebook": {}"#,
        );
        let f = SourceFile::from_ipynb_json(Path::new("a.ipynb"), &by_notebook).unwrap();
        assert!(f.is_databricks());
        // A plain module is neither a notebook nor Databricks.
        let f = SourceFile::from_text(Path::new("a.py"), "%pip install x\n".into());
        assert!(!f.is_databricks() && !f.is_notebook());
        assert_eq!(f.py_source_type(), PySourceType::Python);
    }

    #[test]
    fn line_index_columns_count_chars() {
        let t = "é = 1\nab";
        let idx = LineIndex::new(t);
        assert_eq!(idx.line_col(t, TextSize::from(3)), (1, 3));
        assert_eq!(idx.line_text(t, 2), "ab");
        assert_eq!(idx.line_count(), 2);
    }
}
