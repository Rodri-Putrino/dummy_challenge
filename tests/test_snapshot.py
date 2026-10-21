from sales_pipeline.ingest import snapshot


def test_snapshot_keeps_exact_bytes_even_if_input_is_overwritten(tmp_path):
    source = tmp_path / "input.csv"
    original = b'\xef\xbb\xbftransaction_id,amount\r\n"a",$1.00\r\n'
    source.write_bytes(original)
    first, digest = snapshot(source, tmp_path / "archive")
    source.write_text("changed")
    second, other_digest = snapshot(source, tmp_path / "archive")
    assert first.read_bytes() == original
    assert second.read_text() == "changed"
    assert digest != other_digest
