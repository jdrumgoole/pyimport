# PyImport Documentation

This directory contains the complete Sphinx documentation for PyImport.

## Quick Start

### Build Documentation Locally

```bash
# From project root
cd docs
poetry run sphinx-build -b html . _build/html

# View in browser
open _build/html/index.html  # macOS
xdg-open _build/html/index.html  # Linux
```

### Or Use Make

```bash
cd docs
make html
open _build/html/index.html
```

## Documentation Structure

```
docs/
├── conf.py                    # Sphinx configuration
├── index.rst                  # Main documentation index
├── markdown/                  # All content in Markdown
│   ├── introduction.md        # Overview (118 lines)
│   ├── installation.md        # Setup guide (242 lines)
│   ├── quickstart.md          # Getting started (324 lines)
│   ├── cli_reference.md       # Complete CLI reference (687 lines)
│   ├── fieldfiles.md          # Field file guide (663 lines)
│   └── advanced.md            # Advanced features (668 lines)
├── _build/                    # Generated HTML (gitignored)
├── _static/                   # Static assets
└── _templates/                # Sphinx templates
```

## Documentation Coverage

- **Total:** 2,700+ lines of documentation
- **Examples:** 200+ code snippets
- **Tables:** 10+ reference tables
- **Options:** All 45+ CLI options documented

### Topics Covered

1. **Introduction** - What is PyImport, features, comparison with mongoimport
2. **Installation** - Setup, MongoDB/PostgreSQL, configuration, troubleshooting
3. **Quick Start** - Step-by-step guide, common scenarios, examples
4. **CLI Reference** - Complete reference for all command-line options
5. **Field Files** - Complete guide to `.tff` files and type conversion
6. **Advanced Usage** - Parallel processing, optimization, production examples

## Technology Stack

- **Sphinx 8.x** - Documentation generator
- **MyST Parser** - Markdown support for Sphinx
- **Alabaster** - Clean, professional theme
- **Python 3.11+** - Build environment

## Read the Docs

Documentation is published to Read the Docs:

- **Latest (dev):** https://pyimport.readthedocs.io/en/latest/
- **Stable (release):** https://pyimport.readthedocs.io/en/stable/

### Configuration Files

- **`.readthedocs.yaml`** - Read the Docs build configuration (project root)
- **`requirements-docs.txt`** - Documentation dependencies (project root)

See [READTHEDOCS_SETUP.md](../READTHEDOCS_SETUP.md) for complete setup guide.

## Version Management

Documentation version is **dynamically imported** from `pyimport/version.py`:

```python
# In docs/conf.py
from pyimport.version import __VERSION__
release = __VERSION__
```

**Benefits:**
- No manual version sync needed
- Documentation always shows current version
- Single source of truth

## Building Different Formats

### HTML (Default)

```bash
sphinx-build -b html . _build/html
```

### PDF (via LaTeX)

```bash
sphinx-build -b latex . _build/latex
cd _build/latex && make
```

### ePub

```bash
sphinx-build -b epub . _build/epub
```

### Plain Text

```bash
sphinx-build -b text . _build/text
```

## Dependencies

### Core Dependencies

Required for building documentation:

```txt
sphinx>=8.0.2
myst-parser>=4.0.0
alabaster>=0.7.13
```

### Runtime Dependencies

Required for importing `pyimport.version`:

```txt
pymongo>=4.7.3
python-dateutil>=2.9.0
toml>=0.10.2
configargparse>=1.7
motor>=3.4.0
```

All dependencies specified in `requirements-docs.txt` (project root).

## Contributing to Documentation

### Adding New Pages

1. Create Markdown file in `docs/markdown/`
2. Add to `index.rst` toctree:
   ```rst
   .. toctree::
      :maxdepth: 2

      markdown/your_new_page
   ```
3. Build and test locally
4. Submit pull request

### Documentation Style

- Use **Markdown** (not RST) for content
- Include code examples for all features
- Add troubleshooting sections
- Use tables for comparisons
- Include performance data where relevant

### Example Structure

```markdown
# Page Title

Brief introduction.

## Main Section

Explanation with examples.

### Subsection

```bash
# Example command
pyimport --option data.csv
```

### Troubleshooting

**Problem:** Description

**Solution:** Steps to fix
```

## Testing Documentation

### Local Build Test

```bash
cd docs
poetry run sphinx-build -b html . _build/html
# Check for errors and warnings
```

### Check Links

```bash
poetry run sphinx-build -b linkcheck . _build/linkcheck
cat _build/linkcheck/output.txt
```

### Check Coverage

```bash
poetry run sphinx-build -b coverage . _build/coverage
cat _build/coverage/python.txt
```

## Maintenance

### Updating Documentation

1. Edit Markdown files in `docs/markdown/`
2. Test build locally
3. Commit and push
4. Read the Docs builds automatically

### Updating Version

Version is automatic! Just update `pyimport/version.py`:

```python
__VERSION__: str = "1.10.0"
```

Documentation will automatically show new version on next build.

### Updating Examples

Keep examples current:
- Test all code examples
- Update version numbers in example outputs
- Verify all links work
- Update performance benchmarks periodically

## Troubleshooting

### Build Warnings

Check `_build/html/output.txt` for warnings:

```bash
poetry run sphinx-build -b html . _build/html 2>&1 | grep -i warning
```

Common warnings:
- `Pygments lexer name 'csv' is not known` - Safe to ignore
- `Lexing literal_block resulted in an error` - Use different code block language

### Import Errors

If Sphinx can't import `pyimport.version`:

1. Check Python path in `conf.py`:
   ```python
   sys.path.insert(0, os.path.abspath('..'))
   ```
2. Ensure minimal dependencies installed:
   ```bash
   pip install pymongo python-dateutil toml configargparse motor
   ```

### Missing Dependencies

```bash
pip install -r ../requirements-docs.txt
pip install -e ..
```

## Resources

- **Sphinx:** https://www.sphinx-doc.org/
- **MyST Parser:** https://myst-parser.readthedocs.io/
- **Read the Docs:** https://docs.readthedocs.io/
- **Markdown Guide:** https://www.markdownguide.org/

## Summary

✅ **Complete:** 2,700+ lines covering all features
✅ **Markdown:** Easy to read and edit
✅ **Tested:** Builds successfully with Sphinx
✅ **Versioned:** Auto-imports from `pyimport/version.py`
✅ **Published:** Ready for Read the Docs
✅ **Searchable:** Full-text search enabled
✅ **Multiple Formats:** HTML, PDF, ePub

Documentation is production-ready!
