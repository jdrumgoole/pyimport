# TFF v2.0: Next Steps and Roadmap

## Current Status ✅

**Phase 1 Complete**:
- ✅ Core nested mapping with dot notation
- ✅ 100% test coverage on new code
- ✅ 100% backward compatibility
- ✅ Bug fixes (enricher, PyMongo compatibility)
- ✅ 80 comprehensive tests
- ✅ Production-ready implementation

## Immediate Next Steps (Before Release)

### 1. Version Bump and Release Preparation (1-2 hours)

**Actions**:
```bash
# Update version to 2.0.0 (major version for new feature)
# Edit pyimport/version.py and pyproject.toml
# Current: 1.10.9 → New: 2.0.0
```

**Files to update**:
- [ ] `pyimport/version.py` - Set to "2.0.0"
- [ ] `pyproject.toml` - Set to "2.0.0"
- [ ] `CHANGELOG.md` - Add v2.0.0 release notes (create if needed)

**Release Notes Content**:
- New: TFF v2.0 format with nested document mapping
- New: Dot notation path support for nested fields
- Enhanced: 100% backward compatibility maintained
- Fixed: Enricher handling of nested documents
- Fixed: PyMongo journal parameter compatibility

### 2. Documentation Updates (2-3 hours)

**Update `docs/markdown/fieldfiles.md`**:
- [ ] Add v2.0 format section
- [ ] Add dot notation syntax documentation
- [ ] Add migration guide (v1.0 → v2.0)
- [ ] Add examples for common patterns
- [ ] Add troubleshooting section

**Create `docs/markdown/v2_migration_guide.md`**:
- [ ] Benefits of v2.0 format
- [ ] When to use v2.0 vs v1.0
- [ ] Step-by-step migration examples
- [ ] Common patterns and best practices

**Update `README.md`**:
- [ ] Add v2.0 feature highlight
- [ ] Add quick example of nested mapping
- [ ] Update feature list

**Update `docs/markdown/API.md`**:
- [ ] Document new FieldFile methods (`path_value()`, `is_v2_format()`, `get_field_paths()`)
- [ ] Document FieldPathMapper class
- [ ] Document NestedDocumentBuilder utilities

### 3. CLI Enhancements (Optional, 2-3 hours)

**Add `--validate-fieldfile` flag**:
```python
# Validates TFF file without importing
pyimport --validate-fieldfile mydata.tff
# Output: Valid v2.0 format. 10 fields mapped to nested paths. No conflicts detected.
```

**Add `--preview-structure` flag**:
```python
# Preview document structure without importing
pyimport --preview-structure mydata.csv mydata.tff --limit 3
# Shows first 3 documents with nested structure
```

**Enhance `--genfieldfile` output**:
- Add comments suggesting common nested patterns
- Detect related fields (e.g., first_name/last_name → name.first/name.last)

### 4. Testing and Validation (1-2 hours)

**Run full test suite**:
```bash
invoke test-all
```

**Test with real data**:
- [ ] Import the AandEData.csv with v2.0 TFF
- [ ] Verify MongoDB queries work correctly
- [ ] Test with large dataset (10k+ rows)

**Performance benchmarks**:
- [ ] Compare v1.0 vs v2.0 import speed
- [ ] Document any performance differences
- [ ] Ensure < 5% overhead for v2.0

### 5. Publish Release (30 minutes)

```bash
# Run tests
invoke full-pytest-parallel

# Build package
poetry build

# Publish to PyPI
invoke publish

# Tag release
git tag v2.0.0
git push origin v2.0.0

# Trigger Read the Docs build
invoke trigger-rtd-build
```

## Short-Term Enhancements (Phase 1.5 - Optional)

### Enhancement 1: Auto-detect Nested Patterns (1-2 days)

When generating field files, suggest nested patterns:

```toml
# Old --genfieldfile output:
[first_name]
type = "str"
name = "first_name"
format = ""

# New --genfieldfile output with suggestions:
[first_name]
type = "str"
name = "first_name"
format = ""
# Suggestion: Could map to "name.first" for nested structure

[last_name]
type = "str"
name = "last_name"
format = ""
# Suggestion: Could map to "name.last" for nested structure
```

### Enhancement 2: Field File Converter Tool (1 day)

```bash
# Convert v1.0 TFF to v2.0 with suggested nested structure
pyimport --convert-fieldfile mydata.tff --output mydata_v2.tff
```

### Enhancement 3: MongoDB Index Recommendations (1 day)

Suggest indexes for nested fields:
```python
# After import, suggest:
# For efficient queries, create indexes:
db.collection.createIndex({"personal.name.last": 1})
db.collection.createIndex({"address.city": 1, "address.state": 1})
```

## Medium-Term (Phase 2 - 1-2 weeks)

### Phase 2 Features (from TFF_MAPPING_DESIGN.md)

**Priority 1: Field Composition** (3-4 days)
- Combine multiple CSV fields into single output field
- Template-based composition: `"{first_name} {last_name}"`
- Structure-based composition for objects

**Priority 2: Array Field Support** (2-3 days)
- Multiple columns into array
- Split delimited values into array
- Configurable item types and transformations

**Priority 3: Conditional Mapping** (2 days)
- Value-based transformations
- Conditional field inclusion
- Mapping dictionaries

**Priority 4: Computed Fields** (2 days)
- Expression-based computed fields
- Built-in transforms (upper, lower, strip, etc.)
- Arithmetic operations

### Phase 2 Implementation Plan

1. **Design Review** (1 day)
   - Review TFF_MAPPING_DESIGN.md
   - Prioritize features based on user feedback
   - Create detailed specifications

2. **Implementation** (1 week)
   - Extend FieldFile parser for new syntax
   - Implement field composition
   - Implement array support
   - Add tests for each feature

3. **Testing** (2 days)
   - Unit tests for new features
   - Integration tests
   - Performance testing
   - Documentation

4. **Release v2.1.0** (1 day)
   - Update docs
   - Release notes
   - Publish

## Long-Term Vision (Phase 3+ - Future)

### Advanced Features

**Schema Validation** (Phase 3):
- JSON Schema validation for output documents
- MongoDB schema validation integration
- Validation error reporting

**Template System** (Phase 3):
- Reusable nested structure templates
- Template library for common patterns
- Template inheritance

**Data Transformation Library** (Phase 3):
- Rich set of transformation functions
- Custom transformation plugins
- JMESPath or similar query language support

**Multi-Format Support** (Phase 4):
- JSON input support
- XML input support
- Parquet input support
- Generic format plugins

**Visual Tools** (Phase 4):
- Web-based TFF editor
- Visual field mapper
- Preview/debug tool
- Structure visualizer

## User Feedback and Priorities

### How to Gather Feedback

1. **GitHub Discussions**:
   - Create discussion for v2.0 feedback
   - Ask users about most-wanted Phase 2 features

2. **Documentation**:
   - Add "Request a Feature" section
   - Link to GitHub issues

3. **Real-World Usage**:
   - Monitor GitHub issues for common patterns
   - Track which features are most requested

### Prioritization Criteria

Features will be prioritized based on:
1. **User demand** - How many users request it
2. **Implementation complexity** - Effort required
3. **Value delivered** - Impact on user workflows
4. **Backward compatibility** - Risk to existing users

## Decision Points

### Before Release v2.0.0

**Question 1**: Should we version as 2.0.0 or 1.11.0?
- **Recommendation**: 2.0.0 (major new feature, semantic versioning)
- **Rationale**: Signals significant new capability

**Question 2**: Should we add CLI enhancements now or later?
- **Recommendation**: Later (2.1.0)
- **Rationale**: Get v2.0 core out first, gather feedback

**Question 3**: How much documentation is enough for v2.0?
- **Minimum**: Update fieldfiles.md with v2.0 section + examples
- **Ideal**: Also add migration guide and API docs
- **Recommendation**: Start with minimum, expand based on user questions

**Question 4**: Should we announce v2.0 widely or soft launch?
- **Recommendation**: Soft launch first
- **Rationale**: Let early adopters test, fix any issues, then announce

### For Phase 2

**Question 1**: Which Phase 2 feature first?
- **Wait for user feedback** after v2.0 release
- **Default priority**: Field composition (most commonly requested pattern)

**Question 2**: Should Phase 2 be v2.1 or v3.0?
- **Recommendation**: v2.1 (additive features, backward compatible)
- **Only v3.0 if**: Breaking changes required

## Recommended Action Plan

### Week 1: Release v2.0.0

**Day 1-2**: Documentation
- Update fieldfiles.md
- Add examples and patterns
- Update README

**Day 3**: Testing & validation
- Full test suite
- Real data testing
- Performance benchmarks

**Day 4**: Release prep
- Version bump
- Changelog
- Release notes

**Day 5**: Release
- Publish to PyPI
- Tag GitHub release
- Trigger RTD build
- Monitor for issues

### Week 2-3: Gather Feedback

- Monitor GitHub issues
- Respond to user questions
- Document common patterns
- Collect feature requests for Phase 2

### Week 4+: Plan Phase 2

- Review feedback
- Prioritize features
- Design Phase 2 implementation
- Begin development based on priorities

## Success Metrics

**v2.0 Adoption**:
- Number of downloads post-release
- GitHub stars/watchers increase
- User-reported success stories
- Issues opened (indicates usage)

**Quality Metrics**:
- Bug reports vs. enhancement requests
- Test coverage maintained > 95%
- Documentation completeness score
- User satisfaction feedback

**Phase 2 Direction**:
- Most requested features from users
- Common use cases identified
- Pain points discovered

## Resources Needed

### For v2.0 Release

- **Time**: ~8-12 hours total
- **Skills**: Documentation, testing, release management
- **Tools**: Existing infrastructure (Poetry, PyPI, RTD)

### For Phase 2

- **Time**: ~1-2 weeks development
- **Skills**: Python, TOML parsing, MongoDB, testing
- **Research**: User feedback, common patterns

## Contact and Support

After v2.0 release:
- GitHub Issues for bug reports
- GitHub Discussions for feature requests
- Documentation for common questions
- Examples repository for common patterns

---

## Quick Start Checklist

Ready to release v2.0? Here's your checklist:

- [ ] Update version numbers (2.0.0)
- [ ] Write CHANGELOG entry
- [ ] Update fieldfiles.md documentation
- [ ] Add v2.0 examples to README
- [ ] Run full test suite (invoke test-all)
- [ ] Test with real data
- [ ] Build package (poetry build)
- [ ] Publish to PyPI (invoke publish)
- [ ] Tag release (git tag v2.0.0)
- [ ] Push tag (git push origin v2.0.0)
- [ ] Trigger RTD build (invoke trigger-rtd-build)
- [ ] Monitor for issues
- [ ] Celebrate! 🎉

---

**Last Updated**: 2025-10-13
**Status**: Phase 1 Complete, Ready for v2.0 Release
