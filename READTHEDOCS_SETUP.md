# Read the Docs Setup Guide

Complete guide for publishing PyImport documentation to [Read the Docs](https://readthedocs.org).

## Overview

PyImport documentation is configured to build automatically on Read the Docs using:
- **Sphinx** for documentation generation
- **MyST Parser** for Markdown support
- **Alabaster** theme for styling
- **Python 3.11** build environment

## Configuration Files

### 1. `.readthedocs.yaml`

Main configuration file for Read the Docs build system.

**Location:** Project root (`.readthedocs.yaml`)

**Key settings:**
- Build OS: Ubuntu 22.04
- Python version: 3.11
- Sphinx configuration: `docs/conf.py`
- Output formats: HTML, PDF, ePub
- Requirements: `requirements-docs.txt`

### 2. `requirements-docs.txt`

Documentation build dependencies.

**Location:** Project root (`requirements-docs.txt`)

**Contents:**
- Sphinx >= 8.0.2
- myst-parser >= 4.0.0 (Markdown support)
- alabaster >= 0.7.13 (theme)
- Minimal runtime dependencies (for version import)

### 3. `docs/conf.py`

Sphinx configuration with dynamic version import.

**Key features:**
- Imports version from `pyimport/version.py`
- MyST Parser enabled for Markdown
- Alabaster theme configured

## Setup on Read the Docs

### Step 1: Import Project

1. Go to [readthedocs.org](https://readthedocs.org)
2. Sign in with GitHub account
3. Click "Import a Project"
4. Select `pyimport` repository from the list
5. Click "Import"

### Step 2: Project Settings

Read the Docs will auto-detect configuration from `.readthedocs.yaml`.

**Default settings to verify:**
- ✅ Name: `pyimport`
- ✅ Repository: `https://github.com/jdrumgoole/pyimport`
- ✅ Default branch: `main` (or `master`)
- ✅ Language: `en` (English)

### Step 3: Build Documentation

First build will start automatically. Monitor at:
```
https://readthedocs.org/projects/pyimport/builds/
```

Build process:
1. Clone repository
2. Install Python 3.11
3. Install dependencies from `requirements-docs.txt`
4. Install package with `pip install .`
5. Run `sphinx-build -b html docs docs/_build/html`
6. Publish HTML, PDF, and ePub

### Step 4: Verify Build

Check that build succeeds:
- ✅ Build status: **Passed**
- ✅ No errors in build log
- ✅ Version shows "1.9.0"

View documentation at:
```
https://pyimport.readthedocs.io/en/latest/
```

## Advanced Settings (Optional)

### Custom Domain

**Admin → Domains:**
1. Add custom domain: `docs.pyimport.org`
2. Add DNS CNAME record pointing to `readthedocs.io`
3. Verify and enable

### Subproject Setup

If you have related projects:

**Admin → Advanced Settings → Subprojects:**
- Add subproject relationships
- Share search index

### PR Preview Builds

Enable builds for pull requests:

**Admin → Advanced Settings:**
- ✅ Build pull requests for this project

This creates preview builds at:
```
https://pyimport.readthedocs.io/en/pr-123/
```

### Version Management

**Admin → Versions:**

Active versions:
- ✅ `latest` (tracks `main` branch)
- ✅ `stable` (tracks latest release tag)
- ✅ Version tags (e.g., `v1.9.0`, `v1.8.2`)

## Webhook Setup

Read the Docs automatically sets up GitHub webhook for:
- Push events
- Tag creation
- Pull requests

Verify webhook at:
```
GitHub → Settings → Webhooks
```

Should see webhook pointing to:
```
https://readthedocs.org/api/v2/webhook/pyimport/...
```

## Troubleshooting

### Build Fails: "Module not found"

**Problem:** Can't import `pyimport.version`

**Solution:** Ensure `requirements-docs.txt` includes minimal runtime dependencies:
```txt
pymongo>=4.7.3
python-dateutil>=2.9.0
toml>=0.10.2
configargparse>=1.7
motor>=3.4.0
```

### Build Fails: "Sphinx warnings"

**Problem:** Warnings treated as errors

**Solution:** Set `fail_on_warning: false` in `.readthedocs.yaml`:
```yaml
sphinx:
  fail_on_warning: false
```

### Wrong Version Displayed

**Problem:** Documentation shows old version

**Solution:** Check `pyimport/version.py` is committed with correct version.

### PDF Build Fails

**Problem:** LaTeX errors in PDF generation

**Solution:**
1. Check for unsupported Markdown syntax in docs
2. Disable PDF builds if not needed:
```yaml
formats:
  - epub  # Remove 'pdf'
```

### Build Timeout

**Problem:** Build exceeds 15-minute limit

**Solution:**
1. Reduce dependencies in `requirements-docs.txt`
2. Use faster package resolution
3. Contact Read the Docs support for increased limit

## Local Testing

Test Read the Docs build locally before pushing:

### 1. Install Dependencies

```bash
pip install -r requirements-docs.txt
pip install -e .
```

### 2. Build Documentation

```bash
cd docs
sphinx-build -b html . _build/html
```

### 3. Check Output

```bash
# Open in browser
open _build/html/index.html  # macOS
xdg-open _build/html/index.html  # Linux
```

Verify:
- ✅ All pages load
- ✅ Navigation works
- ✅ Version shows correctly
- ✅ Code examples render properly
- ✅ No broken links

### 4. Test PDF Build (Optional)

```bash
sphinx-build -b latex . _build/latex
cd _build/latex
make
```

## Maintenance

### Updating Documentation

1. Edit Markdown files in `docs/markdown/`
2. Test locally with `sphinx-build`
3. Commit and push to GitHub
4. Read the Docs builds automatically

### Updating Version

1. Update `pyimport/version.py`:
   ```python
   __VERSION__: str = "1.10.0"
   ```
2. Update `pyproject.toml`:
   ```toml
   version = "1.10.0"
   ```
3. Commit and push
4. Read the Docs rebuilds with new version

### Creating Release Docs

For versioned documentation:

1. Tag release:
   ```bash
   git tag -a v1.9.0 -m "Release 1.9.0"
   git push origin v1.9.0
   ```

2. Read the Docs automatically:
   - Creates new version `v1.9.0`
   - Updates `stable` to point to latest tag
   - Keeps `latest` tracking `main` branch

3. Verify at:
   ```
   https://pyimport.readthedocs.io/en/stable/  # Latest release
   https://pyimport.readthedocs.io/en/v1.9.0/  # Specific version
   https://pyimport.readthedocs.io/en/latest/  # Development
   ```

## URLs

Once configured:

- **Latest docs (dev):** https://pyimport.readthedocs.io/en/latest/
- **Stable docs (release):** https://pyimport.readthedocs.io/en/stable/
- **Version docs:** https://pyimport.readthedocs.io/en/v1.9.0/
- **Project page:** https://readthedocs.org/projects/pyimport/
- **Build logs:** https://readthedocs.org/projects/pyimport/builds/

## Badge for README

Add Read the Docs badge to `README.md`:

```markdown
[![Documentation Status](https://readthedocs.org/projects/pyimport/badge/?version=latest)](https://pyimport.readthedocs.io/en/latest/?badge=latest)
```

Renders as:
![Documentation Status](https://readthedocs.org/projects/pyimport/badge/?version=latest)

## Search Configuration

Read the Docs provides built-in search powered by Elasticsearch.

**Features:**
- Full-text search across all documentation
- Search within specific versions
- Search suggestions
- Code search

No configuration needed - works automatically!

## Analytics

View documentation analytics:

**Admin → Traffic Analytics:**
- Page views
- Top pages
- Search queries
- Geographic distribution
- Traffic sources

## Notifications

Configure build notifications:

**Admin → Notifications:**
- ✅ Email on build failures
- ✅ Webhook for build status
- ✅ Slack integration (optional)

## Best Practices

### 1. Version Documentation Changes

Always document what changed in each version:
- Update docs with new features
- Mark deprecated features
- Add migration guides

### 2. Test Locally First

Always build and test locally before pushing:
```bash
cd docs && sphinx-build -b html . _build/html
```

### 3. Keep Dependencies Minimal

Only include essential packages in `requirements-docs.txt`:
- Faster builds
- Fewer version conflicts
- More reliable builds

### 4. Use Semantic Versioning

Tag releases consistently:
```bash
git tag -a v1.9.0 -m "Release 1.9.0"
```

This creates versioned documentation automatically.

### 5. Monitor Build Status

Check build status regularly:
- Subscribe to build failure emails
- Review build logs for warnings
- Update dependencies as needed

## Security

### API Tokens

Keep Read the Docs API tokens secure:
- Store in environment variables
- Never commit to repository
- Rotate regularly

### Webhook Secrets

Verify webhook requests use shared secret:
- Set in Read the Docs admin
- Verify signature in webhook handler

## Support

### Documentation Issues

Report documentation issues:
- GitHub Issues: https://github.com/jdrumgoole/pyimport/issues
- Tag with `documentation` label

### Read the Docs Issues

For Read the Docs platform issues:
- Read the Docs Support: https://docs.readthedocs.io/en/stable/support.html
- Community Forum: https://stackoverflow.com/questions/tagged/read-the-docs

## Resources

- **Read the Docs Documentation:** https://docs.readthedocs.io/
- **Sphinx Documentation:** https://www.sphinx-doc.org/
- **MyST Parser:** https://myst-parser.readthedocs.io/
- **reStructuredText vs Markdown:** https://www.ericholscher.com/blog/2016/mar/15/dont-use-markdown-for-technical-docs/

## Checklist

Before going live:

- [x] `.readthedocs.yaml` created and configured
- [x] `requirements-docs.txt` with minimal dependencies
- [x] `docs/conf.py` imports version dynamically
- [x] Documentation builds locally without errors
- [x] All links work
- [x] Code examples are correct
- [x] Version shows correctly (1.9.0)
- [ ] Project imported to Read the Docs
- [ ] First build succeeds
- [ ] Documentation is publicly accessible
- [ ] Badge added to README.md
- [ ] Custom domain configured (optional)

## Summary

PyImport is ready for Read the Docs! The configuration is complete:

1. ✅ `.readthedocs.yaml` - Build configuration
2. ✅ `requirements-docs.txt` - Dependencies
3. ✅ `docs/conf.py` - Sphinx with dynamic version
4. ✅ Comprehensive Markdown documentation
5. ✅ Local build tested and working

**Next step:** Import project on readthedocs.org and start first build!
