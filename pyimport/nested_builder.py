"""
Nested document builder for TFF v2.0 format.

Provides utilities to build nested MongoDB documents from flat CSV data
using path-based field mapping (dot notation).
"""
from typing import Any, Dict


class NestedDocumentBuilder:
    """Build nested documents from flat key-value pairs using dot-notation paths."""

    @staticmethod
    def set_nested_value(doc: Dict[str, Any], path: str, value: Any) -> None:
        """
        Set a value in a nested document using dot notation path.

        Args:
            doc: The document to modify (modified in place)
            path: Dot-notation path (e.g., "address.city")
            value: The value to set

        Example:
            >>> doc = {}
            >>> NestedDocumentBuilder.set_nested_value(doc, "address.city", "Boston")
            >>> doc
            {'address': {'city': 'Boston'}}
        """
        if not path:
            raise ValueError("Path cannot be empty")

        parts = path.split('.')
        current = doc

        # Navigate/create nested structure
        for i, part in enumerate(parts[:-1]):
            if part not in current:
                current[part] = {}
            elif not isinstance(current[part], dict):
                # Path conflict: intermediate value is not a dict
                existing_path = '.'.join(parts[:i+1])
                raise ValueError(
                    f"Path conflict at '{existing_path}': "
                    f"Cannot create nested path '{path}' because '{existing_path}' "
                    f"is already set to a non-dict value: {current[part]}"
                )
            current = current[part]

        # Set the final value
        final_key = parts[-1]
        if final_key in current and isinstance(current[final_key], dict):
            # Path conflict: trying to set a dict to a scalar
            raise ValueError(
                f"Path conflict at '{path}': "
                f"'{path}' is already a nested object, cannot overwrite with scalar value"
            )

        current[final_key] = value

    @staticmethod
    def build_nested_doc(flat_doc: Dict[str, Any], field_paths: Dict[str, str]) -> Dict[str, Any]:
        """
        Build a nested document from a flat document using field path mappings.

        Args:
            flat_doc: Flat document with CSV field names as keys
            field_paths: Mapping from CSV field name to nested path
                        e.g., {"first_name": "personal.name.first"}

        Returns:
            Nested document with values placed at specified paths

        Example:
            >>> flat_doc = {"first_name": "John", "city": "Boston"}
            >>> field_paths = {"first_name": "name.first", "city": "address.city"}
            >>> NestedDocumentBuilder.build_nested_doc(flat_doc, field_paths)
            {'name': {'first': 'John'}, 'address': {'city': 'Boston'}}
        """
        nested_doc = {}

        for field_name, value in flat_doc.items():
            if field_name in field_paths:
                # Field has a path mapping - use nested structure
                path = field_paths[field_name]
                NestedDocumentBuilder.set_nested_value(nested_doc, path, value)
            else:
                # Field has no path mapping - use as top-level field (v1.0 compatibility)
                nested_doc[field_name] = value

        return nested_doc

    @staticmethod
    def validate_paths(field_paths: Dict[str, str]) -> None:
        """
        Validate that field paths don't conflict with each other.

        A conflict occurs when:
        - One path is a prefix of another (e.g., "address" and "address.city")
        - Two paths would create incompatible structures

        Args:
            field_paths: Mapping from field name to nested path

        Raises:
            ValueError: If paths conflict

        Example:
            >>> paths = {"field1": "address", "field2": "address.city"}
            >>> NestedDocumentBuilder.validate_paths(paths)
            ValueError: Path conflict: 'address' and 'address.city' are incompatible
        """
        paths = list(field_paths.values())

        # Check for prefix conflicts
        for i, path1 in enumerate(paths):
            for path2 in paths[i+1:]:
                # Check if one path is a prefix of another
                if path1.startswith(path2 + '.') or path2.startswith(path1 + '.'):
                    raise ValueError(
                        f"Path conflict: '{path1}' and '{path2}' are incompatible. "
                        f"One path is a prefix of the other, which would create "
                        f"a structure where a field is both a scalar and an object."
                    )

                # Check for exact duplicates
                if path1 == path2:
                    # Find which fields map to this path
                    fields = [k for k, v in field_paths.items() if v == path1]
                    raise ValueError(
                        f"Duplicate path '{path1}' used for multiple fields: {fields}. "
                        f"Each path must be unique."
                    )


class FieldPathMapper:
    """Helper class to manage field path mappings for a FieldFile."""

    def __init__(self, field_file):
        """
        Initialize mapper from a FieldFile.

        Args:
            field_file: FieldFile instance (v1.0 or v2.0 format)
        """
        self._field_file = field_file
        self._field_paths = self._extract_paths()
        self._is_v2 = len(self._field_paths) > 0

        # Validate paths if v2.0
        if self._is_v2:
            NestedDocumentBuilder.validate_paths(self._field_paths)

    def _extract_paths(self) -> Dict[str, str]:
        """
        Extract path mappings from field file.

        Returns:
            Dict mapping CSV field name to nested path (only for fields with 'path' defined)
        """
        paths = {}
        for field_name in self._field_file.fields():
            field_config = self._field_file.field_dict[field_name]
            if isinstance(field_config, dict) and 'path' in field_config:
                paths[field_name] = field_config['path']
        return paths

    @property
    def is_v2_format(self) -> bool:
        """Returns True if this field file uses v2.0 format (has path mappings)."""
        return self._is_v2

    @property
    def field_paths(self) -> Dict[str, str]:
        """Returns the field path mappings."""
        return self._field_paths

    def build_document(self, flat_doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build document from flat CSV data.

        If v2.0 format (has paths), builds nested document.
        If v1.0 format (no paths), returns flat document unchanged.

        Args:
            flat_doc: Flat document from CSV reader

        Returns:
            Document (nested if v2.0, flat if v1.0)
        """
        if self._is_v2:
            return NestedDocumentBuilder.build_nested_doc(flat_doc, self._field_paths)
        else:
            return flat_doc
