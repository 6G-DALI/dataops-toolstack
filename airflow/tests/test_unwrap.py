"""
Tests for dali.unwrap (narrow <-> one column per series), the layout a distribution declares in piveau, and the way
dali.processing wraps the pipeline's output back.

Run with:
    cd airflow
    PYTHONPATH=plugins pytest tests/test_unwrap.py -v
"""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import pandas as pd
import pytest

from dali.unwrap import UnwrapError, parse_hierarchies, parse_hierarchy, series_name, unwrap, wrap

FIELDS = "cell_id,ue_id,metric_id"                  # the parts in order, as the catalogue holds them
HIERARCHY = "{cell_id}_{ue_id}_{metric_id}"         # the older template form, still read
IGNORE = ["source", "gNB_id"]

# Two cells, one UE under cell 3; the cell rows have no UE (ue_id N/A). source and gNB_id hold one value.
NARROW = """source,metric_id,timestamp,unit,value,cell_id,gNB_id,type,ue_id
Digital_Twin,DRB.UEThpDl,2026-05-28T11:36:54.462892+02:00,N/A,9.5,1,1193046,Cell,N/A
Digital_Twin,Viavi.Geo.x,2026-05-28T11:36:54.462892+02:00,N/A,100.5,1,1193046,Cell,N/A
Digital_Twin,DRB.UEThpDl,2026-05-28T11:36:55.462892+02:00,N/A,10.5,1,1193046,Cell,N/A
Digital_Twin,DRB.UEThpDl,2026-05-28T11:36:54.562892+02:00,N/A,3.5,3,1193046,Cell,N/A
Digital_Twin,DRB.UEBlerDl,2026-05-28T11:36:54.562892+02:00,N/A,0.25,3,1193046,UE,4
Digital_Twin,DRB.UEBlerDl,2026-05-28T11:36:55.562892+02:00,N/A,0.5,3,1193046,UE,4
Digital_Twin,Viavi.Geo.x,2026-05-28T11:36:54.562892+02:00,N/A,7.0,3,1193046,UE,4
"""


@pytest.fixture
def narrow() -> pd.DataFrame:
    return pd.read_csv(io.StringIO(NARROW), dtype=str, keep_default_na=False)


def _canon(frame: pd.DataFrame) -> pd.DataFrame:
    """Order-independent form for comparing two narrow frames."""
    frame = frame.assign(value=pd.to_numeric(frame["value"])).astype(str)
    return frame.sort_values(list(frame.columns)).reset_index(drop=True)


class TestHierarchy:
    def test_fields_and_separator(self):
        assert parse_hierarchy(HIERARCHY) == (["cell_id", "ue_id", "metric_id"], "_")

    @pytest.mark.parametrize("bad", ["", "{a}", "a_{b}_{c}", "{a}_{b}.{c}", "{a}_{a}", "{a}__{b}x"])
    def test_rejects(self, bad):
        with pytest.raises(UnwrapError):
            parse_hierarchy(bad)

    def test_absent_levels_leave_the_name(self):
        assert series_name(["1", "N/A", "DRB.UEThpDl"], "_") == "1_DRB.UEThpDl"
        assert series_name(["3", "4", "DRB.UEBlerDl"], "_") == "3_4_DRB.UEBlerDl"


class TestUnwrap:
    def test_one_column_per_series(self, narrow):
        wide, state = unwrap(narrow, HIERARCHY, IGNORE)
        assert list(wide.columns) == [
            "timestamp",
            "1_DRB.UEThpDl", "1_Viavi.Geo.x",
            "3_DRB.UEThpDl",
            "3_4_DRB.UEBlerDl", "3_4_Viavi.Geo.x",
        ]
        assert state.stats["series"] == 5 and state.stats["ignored_columns"] == IGNORE
        assert state.tz_offset == "+02:00"

    def test_values_land_on_their_timestamps_in_naive_utc(self, narrow):
        wide, _ = unwrap(narrow, HIERARCHY, IGNORE)
        assert wide["timestamp"].tolist() == [
            "2026-05-28T09:36:54.462892", "2026-05-28T09:36:54.562892",
            "2026-05-28T09:36:55.462892", "2026-05-28T09:36:55.562892",
        ]
        assert wide["1_DRB.UEThpDl"].tolist()[0] == 9.5
        assert pd.isna(wide["1_DRB.UEThpDl"].tolist()[1])
        assert wide["3_4_DRB.UEBlerDl"].tolist()[1:4:2] == [0.25, 0.5]

    def test_ignored_columns_are_not_part_of_the_series(self, narrow):
        wide, state = unwrap(narrow, HIERARCHY, IGNORE)
        assert state.constants == {"source": "Digital_Twin", "gNB_id": "1193046"}
        assert not any("1193046" in c or "Digital" in c for c in wide.columns)

    def test_a_column_that_varies_cannot_be_ignored(self, narrow):
        narrow.loc[0, "gNB_id"] = "2"
        with pytest.raises(UnwrapError, match="only a column with one value"):
            unwrap(narrow, HIERARCHY, IGNORE)

    def test_ignoring_an_unknown_or_structural_column(self, narrow):
        with pytest.raises(UnwrapError, match="not in the dataset"):
            unwrap(narrow, HIERARCHY, ["nope"])
        for structural in ("cell_id", "timestamp", "value"):
            with pytest.raises(UnwrapError, match="cannot be ignored"):
                unwrap(narrow, HIERARCHY, [structural])

    def test_a_constant_gnb_left_in_the_hierarchy_would_prefix_every_name(self, narrow):
        wide, _ = unwrap(narrow, "{gNB_id}_{cell_id}_{ue_id}_{metric_id}", ["source"])
        assert "1193046_1_DRB.UEThpDl" in wide.columns

    def test_missing_column(self, narrow):
        with pytest.raises(UnwrapError, match="no column"):
            unwrap(narrow, "{cell_id}_{nope}_{metric_id}")
        with pytest.raises(UnwrapError, match="no column"):
            unwrap(narrow, HIERARCHY, value_col="reading")

    def test_a_column_that_varies_within_a_series_is_refused(self, narrow):
        narrow.loc[0, "unit"] = "Mbps"
        with pytest.raises(UnwrapError, match="not constant per series"):
            unwrap(narrow, HIERARCHY, IGNORE)

    def test_bad_timestamp(self, narrow):
        narrow.loc[0, "timestamp"] = "yesterday"
        with pytest.raises(UnwrapError, match="not a timestamp"):
            unwrap(narrow, HIERARCHY, IGNORE)

    def test_duplicate_samples_keep_the_last(self, narrow):
        narrow = pd.concat([narrow, narrow.iloc[[0]].assign(value="99")], ignore_index=True)
        wide, state = unwrap(narrow, HIERARCHY, IGNORE)
        assert state.stats["duplicate_samples_dropped"] == 1
        assert wide["1_DRB.UEThpDl"].tolist()[0] == 99.0


class TestWrap:
    def test_round_trip_is_lossless(self, narrow):
        wide, state = unwrap(narrow, HIERARCHY, IGNORE)
        back = wrap(wide, state)
        assert list(back.columns) == list(narrow.columns)
        pd.testing.assert_frame_equal(_canon(back), _canon(narrow))

    def test_ignored_columns_are_restored_with_their_value(self, narrow):
        wide, state = unwrap(narrow, HIERARCHY, IGNORE)
        back = wrap(wide, state)
        assert set(back["source"]) == {"Digital_Twin"} and set(back["gNB_id"]) == {"1193046"}

    def test_filled_values_become_rows(self, narrow):
        wide, state = unwrap(narrow, HIERARCHY, IGNORE)
        wide["1_DRB.UEThpDl"] = wide["1_DRB.UEThpDl"].fillna(0.0)          # an "imputation"
        back = wrap(wide, state)
        cell1 = back[(back["cell_id"] == "1") & (back["metric_id"] == "DRB.UEThpDl")]
        assert len(cell1) == 4
        assert set(cell1["type"]) == {"Cell"} and set(cell1["ue_id"]) == {"N/A"}

    def test_epoch_time_and_renamed_columns_as_the_pipeline_leaves_them(self, narrow):
        wide, state = unwrap(narrow, HIERARCHY, IGNORE)
        times = pd.to_datetime(wide["timestamp"])
        clean = lambda c: c.lower().replace(".", "_")                       # what the pipeline's snake_case does
        processed = pd.DataFrame({
            "timestamp": (times - pd.Timestamp("1970-01-01")).dt.total_seconds(),
            "split": ["train", "train", "test", "test"],                    # a column the pipeline adds
            **{clean(c): wide[c] for c in wide.columns[1:]},
        })
        back = wrap(processed, state, rename={clean(c): c for c in wide.columns[1:]}, time_unit="epoch")
        assert back["timestamp"].iloc[0] == "2026-05-28T11:36:54.462892+02:00"
        assert len(back) == len(narrow)

    def test_nothing_recognised(self, narrow):
        wide, state = unwrap(narrow, HIERARCHY, IGNORE)
        with pytest.raises(UnwrapError, match="none of the frame's columns"):
            wrap(wide.rename(columns=lambda c: c if c == "timestamp" else "x" + c), state)


class TestWrapRuns:
    """dali.processing._wrap_runs: the pipeline snake_cases names and writes epoch seconds."""

    def test_a_runs_final_timeline_is_wrapped_back(self, narrow, tmp_path: Path):
        from dali.processing import _wrap_runs
        from dataops.cleaning import build_column_mapping

        wide, state = unwrap(narrow, HIERARCHY, IGNORE)
        mapping = build_column_mapping(list(wide.columns))
        times = pd.to_datetime(wide["timestamp"])
        final = pd.DataFrame({
            "timestamp": (times - pd.Timestamp("1970-01-01")).dt.total_seconds(),
            "split": "train",
            **{mapping[c]: wide[c].fillna(1.0) for c in wide.columns[1:]},
        })
        final_csv = tmp_path / "final.csv"
        final.to_csv(final_csv, index=False)
        prepared = tmp_path / "bundle"
        prepared.mkdir()
        (prepared / "meta.json").write_text(json.dumps({"time_col": "timestamp", "time_unit_seconds": 1.0}))
        imputation = {"lib": "darts", "method": "linear", "final": {"path": str(final_csv)}}

        out = _wrap_runs(imputation, state, list(wide.columns), tmp_path, "ds", str(prepared))

        assert list(out) == ["narrow_darts_linear"]
        result = pd.read_csv(out["narrow_darts_linear"], dtype=str, keep_default_na=False)
        assert list(result.columns) == list(narrow.columns)
        assert len(result) == 5 * 4                                  # 5 series filled over 4 timestamps
        assert set(result["gNB_id"]) == {"1193046"} and set(result["source"]) == {"Digital_Twin"}
        assert result["timestamp"].iloc[0] == "2026-05-28T11:36:54.462892+02:00"
        assert imputation["narrow"]["rows"] == 20

    def test_a_run_that_cannot_be_wrapped_is_recorded_not_raised(self, narrow, tmp_path: Path):
        from dali.processing import _wrap_runs

        wide, state = unwrap(narrow, HIERARCHY, IGNORE)
        bad = tmp_path / "final.csv"
        bad.write_text("a,b\n1,2\n")
        imputation = {"lib": "darts", "method": "linear", "final": {"path": str(bad)}}
        assert _wrap_runs(imputation, state, list(wide.columns), tmp_path, "ds", None) == {}
        assert "error" in imputation["narrow"]


class TestSeriesLayoutFromCatalogue:
    """The layout a distribution declares in piveau, and the precedence against the run's own params."""

    NODE = {
        "@id": "https://dataspace.6gdali.eu/distributions/64d5",
        "@type": "dcat:Distribution",
        "https://dali-project.eu/ns#assetId": "5953562c",
        "https://dali-project.eu/ns#seriesHierarchy": FIELDS,
        "https://dali-project.eu/ns#ignoredColumn": ["source", {"@value": "gNB_id"}],
        "https://dali-project.eu/ns#valueColumn": "value",
    }

    def test_read_from_full_iris(self):
        from dali.utils import series_layout_of

        assert series_layout_of(self.NODE) == {
            "hierarchy": [FIELDS], "separator": "_", "ignore_columns": ["source", "gNB_id"], "value_col": "value"}

    def test_read_from_prefixed_keys_and_a_single_ignored_value(self):
        from dali.utils import series_layout_of

        node = {"@type": "dcat:Distribution", "dali:seriesHierarchy": {"@value": "{a}_{b}"},
                "dali:ignoredColumn": "x", "dali:valueColumn": "reading"}
        assert series_layout_of(node) == {
            "hierarchy": ["{a}_{b}"], "separator": "_", "ignore_columns": ["x"], "value_col": "reading"}

    def test_no_hierarchy_means_no_layout(self):
        from dali.utils import series_layout_of

        assert series_layout_of({"@type": "dcat:Distribution", "dali:ignoredColumn": "x"}) is None

    def test_value_column_defaults(self):
        from dali.utils import series_layout_of

        assert series_layout_of({"dali:seriesHierarchy": "{a}_{b}"})["value_col"] == "value"

    def test_fetch_targets_the_distribution_by_asset_id(self, monkeypatch):
        from dali import utils

        other = {"@id": "x/other", "@type": "dcat:Distribution", "https://dali-project.eu/ns#assetId": "other",
                 "https://dali-project.eu/ns#seriesHierarchy": "{a}_{b}"}
        dataset_node = {"@id": "d", "@type": ["dcat:Dataset"], "dali:seriesHierarchy": "{z}_{y}"}   # never read
        body = json.dumps({"@graph": [dataset_node, other, self.NODE]}).encode()
        seen = {}

        class Resp:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def read(self): return body

        def fake_urlopen(request, timeout=0):
            seen["url"], seen["key"] = request.full_url, request.get_header("X-api-key")
            return Resp()

        monkeypatch.setattr(utils.urllib.request, "urlopen", fake_urlopen)
        monkeypatch.setenv("PIVEAU_API_KEY", "k")
        layout = utils.fetch_series_layout("ds1", "cat1", "5953562c")
        assert layout["hierarchy"] == [FIELDS]
        assert seen["url"].endswith("/ds1?catalogue=cat1") and seen["key"] == "k"
        assert utils.fetch_series_layout("ds1", "cat1", "nope") is None
        assert utils.fetch_series_layout("ds1", "cat1", "") is None        # two distributions, none named

    def test_an_unreadable_catalogue_means_no_layout(self, monkeypatch):
        from dali import utils

        def boom(*a, **k):
            raise OSError("down")

        monkeypatch.setattr(utils.urllib.request, "urlopen", boom)
        assert utils.fetch_series_layout("ds1") is None

    def test_params_win_over_the_catalogue(self, monkeypatch):
        from dali import processing

        monkeypatch.setattr(processing, "fetch_series_layout",
                            lambda *a: {"hierarchy": "{x}_{y}", "ignore_columns": [], "value_col": "value"})
        own = processing.resolve_series_layout({"hierarchy": HIERARCHY, "ignore_columns": ["a"], "dataset_id": "d"})
        assert own == {"hierarchy": HIERARCHY, "separator": "_", "ignore_columns": ["a"], "value_col": "value",
                       "source": "params"}
        declared = processing.resolve_series_layout({"dataset_id": "d", "catalogue_id": "c", "asset_id": "a"})
        assert declared["hierarchy"] == "{x}_{y}" and declared["source"] == "piveau"

    def test_nothing_declared_nothing_unwrapped(self, monkeypatch):
        from dali import processing

        monkeypatch.setattr(processing, "fetch_series_layout", lambda *a: None)
        assert processing.resolve_series_layout({"dataset_id": "d"}) is None
        assert processing.resolve_series_layout({}) is None


class TestSetSeriesLayoutScript:
    SCRIPTS = str(Path(__file__).resolve().parents[1] / "scripts")

    def _graph(self):
        return {"@graph": [
            {"@id": "d", "@type": ["dcat:Dataset"]},
            {"@id": "x1", "@type": "dcat:Distribution", "https://dali-project.eu/ns#assetId": "a1"},
            {"@id": "x2", "@type": "dcat:Distribution", "https://dali-project.eu/ns#assetId": "a2",
             "https://dali-project.eu/ns#ignoredColumn": ["old"]},
        ]}

    def test_sets_only_the_named_distribution_never_the_dataset(self):
        sys.path.insert(0, self.SCRIPTS)
        from set_series_layout import apply_layout
        from dali.utils import series_layout_of

        graph = self._graph()
        change = apply_layout(graph, "a1", FIELDS, IGNORE, "value")
        assert change["before"] == {}
        assert series_layout_of(graph["@graph"][1]) == {
            "hierarchy": [FIELDS], "separator": "_", "ignore_columns": IGNORE, "value_col": "value"}
        assert series_layout_of(graph["@graph"][2]) is None                 # the other distribution is untouched
        assert graph["@graph"][2]["https://dali-project.eu/ns#ignoredColumn"] == ["old"]
        assert not any(k.startswith("https://dali-project.eu/ns#") for k in graph["@graph"][0])   # nor the dataset

    def test_replaces_rather_than_merges_and_clears(self):
        sys.path.insert(0, self.SCRIPTS)
        from set_series_layout import apply_layout

        graph = self._graph()
        apply_layout(graph, "a2", HIERARCHY, ["new"], "value")
        assert graph["@graph"][2]["https://dali-project.eu/ns#ignoredColumn"] == ["new"]
        apply_layout(graph, "a2", None, [], None)
        assert not any(k.startswith("https://dali-project.eu/ns#") and not k.endswith("assetId")
                       for k in graph["@graph"][2])

    def test_unknown_asset(self):
        sys.path.insert(0, self.SCRIPTS)
        from set_series_layout import apply_layout

        with pytest.raises(SystemExit):
            apply_layout(self._graph(), "nope", HIERARCHY, [], "value")


class TestSeveralHierarchies:
    LIST = "{cell_id}_{metric_id},{cell_id}_{ue_id}_{metric_id}"

    def test_parse(self):
        assert parse_hierarchies(self.LIST) == [
            (["cell_id", "metric_id"], "_"), (["cell_id", "ue_id", "metric_id"], "_")]
        assert parse_hierarchies(f" {self.LIST} , ") == parse_hierarchies(self.LIST)
        assert parse_hierarchies([HIERARCHY]) == [(["cell_id", "ue_id", "metric_id"], "_")]

    def test_templates_must_share_the_metric_field(self):
        with pytest.raises(UnwrapError, match="same metric field"):
            parse_hierarchies("{a}_{b}_{metric_id},{a}_{b}_{type}")
        with pytest.raises(UnwrapError, match="empty"):
            parse_hierarchies(" , ")

    def test_each_row_takes_the_template_that_fits_it(self, narrow):
        wide, state = unwrap(narrow, self.LIST, IGNORE)
        assert "3_4_DRB.UEBlerDl" in wide.columns and "3_DRB.UEThpDl" in wide.columns
        assert state.stats["hierarchies"] == ["{cell_id}_{metric_id}", "{cell_id}_{ue_id}_{metric_id}"]

    def test_same_result_as_one_template_with_absent_levels_left_out(self, narrow):
        listed, _ = unwrap(narrow, self.LIST, IGNORE)
        single, _ = unwrap(narrow, HIERARCHY, IGNORE)
        pd.testing.assert_frame_equal(listed, single)

    def test_round_trip(self, narrow):
        wide, state = unwrap(narrow, self.LIST, IGNORE)
        pd.testing.assert_frame_equal(_canon(wrap(wide, state)), _canon(narrow))

    def test_a_ue_row_is_not_swallowed_by_the_shorter_template(self, narrow):
        # listed shortest first: the UE row must still get the three-field name, or UEs of one cell would collide
        wide, _ = unwrap(narrow, self.LIST, IGNORE)
        assert "3_4_Viavi.Geo.x" in wide.columns

    def test_catalogue_value_as_list_or_commas(self):
        from dali.utils import series_layout_of

        a = series_layout_of({"dali:seriesHierarchy": self.LIST})
        b = series_layout_of({"dali:seriesHierarchy": ["{cell_id}_{metric_id}",
                                                       {"@value": "{cell_id}_{ue_id}_{metric_id}"}]})
        assert parse_hierarchies(a["hierarchy"]) == parse_hierarchies(b["hierarchy"]) == parse_hierarchies(self.LIST)


class TestOrderedFieldList:
    """dali:seriesHierarchy as the parts in order, comma-separated, with an optional dali:seriesSeparator."""

    def test_parse_the_parts_and_the_separator(self):
        assert parse_hierarchies(FIELDS) == [(["cell_id", "ue_id", "metric_id"], "_")]
        assert parse_hierarchies(" cell_id , ue_id,metric_id ") == parse_hierarchies(FIELDS)
        assert parse_hierarchies(FIELDS, ".") == [(["cell_id", "ue_id", "metric_id"], ".")]

    def test_several_values_are_several_hierarchies(self):
        assert parse_hierarchies(["cell_id,metric_id", "cell_id,ue_id,metric_id"]) == [
            (["cell_id", "metric_id"], "_"), (["cell_id", "ue_id", "metric_id"], "_")]

    @pytest.mark.parametrize("bad", ["cell_id", "cell_id,cell_id,metric_id"])
    def test_rejects(self, bad):
        with pytest.raises(UnwrapError):
            parse_hierarchies(bad)

    @pytest.mark.parametrize("sep", ["", "{", "a,b"])
    def test_rejects_a_bad_separator(self, sep):
        with pytest.raises(UnwrapError, match="separator"):
            parse_hierarchies(FIELDS, sep)

    def test_same_series_as_the_template_form(self, narrow):
        fields, _ = unwrap(narrow, FIELDS, IGNORE)
        template, _ = unwrap(narrow, HIERARCHY, IGNORE)
        pd.testing.assert_frame_equal(fields, template)

    def test_the_separator_joins_the_names(self, narrow):
        wide, state = unwrap(narrow, FIELDS, IGNORE, separator=".")
        assert "3.4.DRB.UEBlerDl" in wide.columns and "1.DRB.UEThpDl" in wide.columns
        pd.testing.assert_frame_equal(_canon(wrap(wide, state)), _canon(narrow))

    def test_the_separator_from_the_catalogue_reaches_the_run(self):
        from dali import processing

        layout = processing.resolve_series_layout({"hierarchy": FIELDS, "separator": "."})
        assert layout["separator"] == "." and layout["source"] == "params"

    def test_the_script_writes_the_separator_only_when_given(self):
        sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
        from set_series_layout import apply_layout

        graph = {"@graph": [{"@id": "x", "@type": "dcat:Distribution", "https://dali-project.eu/ns#assetId": "a"}]}
        assert "seriesSeparator" not in apply_layout(graph, "a", FIELDS, IGNORE, "value")["after"]
        assert apply_layout(graph, "a", FIELDS, IGNORE, "value", ".")["after"]["seriesSeparator"] == "."
