import pytest

from raft.datastore import parser


@pytest.mark.parametrize(
    "val,expected",
    (
        ("GeT", parser.Command.Get),
        ("seT", parser.Command.Set),
        ("deleTe", parser.Command.Delete),
        ("-", None),
    ),
)
def test_command_read(val, expected):
    if expected is None:
        with pytest.raises(ValueError):
            parser.Command.read(val)
    else:
        result = parser.Command.read(val)
        assert result is expected
        assert str(result).startswith("Command.")
        assert str(result).endswith(expected.name)


@pytest.mark.parametrize(
    "action,expected",
    (
        ("get a b", (parser.Command.Get, ["a", "b"])),
        ("set      a b", (parser.Command.Set, ["a", "b"])),
        ("delete a       b", (parser.Command.Delete, ["a", "b"])),
        ("unknown a b", None),
        ("unknown     ", None),
    ),
)
def test_parse(action, expected):
    assert parser.parse(action) == expected
