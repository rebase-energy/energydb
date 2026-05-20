"""Unit tests for ``_coerce_path`` and its helpers.

Pure-Python; no DB required. Covers the path-string canonical form
(``"P/Site/T01"`` split on ``/``) introduced alongside the existing
variadic and tuple/list forms.
"""

from __future__ import annotations

import pytest
from energydb.scope import _coerce_path, _flatten_segments, _split_path_string


class TestSplitPathString:
    def test_single_segment(self):
        assert _split_path_string("P") == ("P",)

    def test_multiple_segments(self):
        assert _split_path_string("P/Site/T01") == ("P", "Site", "T01")

    def test_unicode_segment(self):
        assert _split_path_string("P/北京/T01") == ("P", "北京", "T01")

    def test_segment_with_dot(self):
        assert _split_path_string("P/N.O.R.D/T01") == ("P", "N.O.R.D", "T01")

    def test_segment_with_space(self):
        assert _split_path_string("P/Lillgrund Park") == ("P", "Lillgrund Park")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            _split_path_string("")

    def test_leading_slash_raises(self):
        with pytest.raises(ValueError, match="empty segment"):
            _split_path_string("/P")

    def test_trailing_slash_raises(self):
        with pytest.raises(ValueError, match="empty segment"):
            _split_path_string("P/")

    def test_double_slash_raises(self):
        with pytest.raises(ValueError, match="empty segment"):
            _split_path_string("P//Site")

    def test_just_a_slash_raises(self):
        with pytest.raises(ValueError, match="empty segment"):
            _split_path_string("/")


class TestFlattenSegments:
    def test_empty(self):
        assert _flatten_segments(()) == ()

    def test_plain_segments(self):
        assert _flatten_segments(("P", "Site", "T01")) == ("P", "Site", "T01")

    def test_string_with_slash_splits(self):
        # Structured form with an embedded separator — still split.
        assert _flatten_segments(("P/Site", "T01")) == ("P", "Site", "T01")

    def test_all_strings_split(self):
        assert _flatten_segments(("P/Site/T01",)) == ("P", "Site", "T01")

    def test_non_string_segment_raises(self):
        with pytest.raises(TypeError, match="must be str"):
            _flatten_segments(("P", 123))

    def test_empty_string_in_segments_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            _flatten_segments(("P", ""))


class TestCoercePath:
    def test_variadic_strings(self):
        assert _coerce_path(("P", "Site", "T01")) == ("P", "Site", "T01")

    def test_canonical_slash_joined_string(self):
        assert _coerce_path(("P/Site/T01",)) == ("P", "Site", "T01")

    def test_variadic_mixed_with_slash(self):
        assert _coerce_path(("P/Site", "T01")) == ("P", "Site", "T01")

    def test_single_tuple_arg(self):
        assert _coerce_path((("P", "Site", "T01"),)) == ("P", "Site", "T01")

    def test_single_list_arg(self):
        assert _coerce_path((["P", "Site", "T01"],)) == ("P", "Site", "T01")

    def test_tuple_arg_with_string_segments_split(self):
        # Strings inside a tuple are still split — full consistency.
        assert _coerce_path((("P/Site", "T01"),)) == ("P", "Site", "T01")

    def test_kwarg_string(self):
        assert _coerce_path((), kwarg="P/Site/T01") == ("P", "Site", "T01")

    def test_kwarg_tuple(self):
        assert _coerce_path((), kwarg=("P", "Site", "T01")) == ("P", "Site", "T01")

    def test_kwarg_list(self):
        assert _coerce_path((), kwarg=["P", "Site", "T01"]) == ("P", "Site", "T01")

    def test_empty_args_empty_kwarg_returns_empty(self):
        # Caller-side empty input: preserves the historical contract that
        # downstream resolvers raise on empty paths, not _coerce_path itself.
        assert _coerce_path(()) == ()

    def test_empty_string_arg_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            _coerce_path(("",))

    def test_leading_slash_raises(self):
        with pytest.raises(ValueError, match="empty segment"):
            _coerce_path(("/P",))

    def test_trailing_slash_raises(self):
        with pytest.raises(ValueError, match="empty segment"):
            _coerce_path(("P/",))

    def test_double_slash_raises(self):
        with pytest.raises(ValueError, match="empty segment"):
            _coerce_path(("P//Site",))

    def test_empty_string_variadic_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            _coerce_path(("P", ""))

    def test_kwarg_empty_string_raises(self):
        with pytest.raises(ValueError, match="non-empty"):
            _coerce_path((), kwarg="")

    def test_non_string_segment_raises(self):
        with pytest.raises(TypeError, match="must be str"):
            _coerce_path(("P", 42))
