import copy
import time

import pytest

from raft.traffic import models


# # # # #
# Model
# # # # #


@pytest.mark.parametrize(
    "val,expected",
    (
        (models.Model.Button_EW, True),
        (models.Model.Button_NS, True),
        (models.Model.Time, False),
        ("", False),
    ),
)
def test_is_button_press(val, expected):
    assert models.Model.is_button_press(val) is expected


@pytest.mark.parametrize(
    "val,expected",
    (
        (models.Model.Time, True),
        (models.Model.Button_NS, False),
        ("", False),
    ),
)
def test_is_clock_tick(val, expected):
    assert models.Model.is_clock_tick(val) is expected


@pytest.mark.parametrize(
    "val,expected",
    (
        (models.BUTTON_EW, models.Model.Button_EW),
        (models.BUTTON_NS, models.Model.Button_NS),
        (models.Model.Time, None),
        ("", None),
    ),
)
def test_read_button(val, expected):
    if expected is None:
        with pytest.raises(ValueError):
            models.Model.read_button(val)
    else:
        assert models.Model.read_button(val) is expected


# # # # #
# Light
# # # # #
def test_light_init():
    with pytest.raises(ValueError):
        models.Light("bla", "")
    with pytest.raises(ValueError):
        models.Light(models.Light.COLOR_G, "")
    with pytest.raises(ValueError):
        models.Light("", models.LIGHT_EW)
    light = models.Light(models.Light.COLOR_G, models.LIGHT_NS)
    assert light.label in repr(light)
    assert not light.button_pressed
    assert 0 < light.age < 100
    assert light.color == models.Light.COLOR_G


@pytest.mark.parametrize(
    "color,label,state_change_time,button_pressed,timeout_expected",
    (
        ("R", models.LIGHT_NS, 100, False, False),
        ("R", models.LIGHT_NS, 1000, False, False),
        ("R", models.LIGHT_NS, 100, True, False),
        ("R", models.LIGHT_EW, 100, False, False),
        ("R", models.LIGHT_EW, 1000, False, False),
        ("R", models.LIGHT_EW, 100, True, False),
        ("Y", models.LIGHT_NS, 2, False, False),
        ("Y", models.LIGHT_NS, 5, False, True),
        ("Y", models.LIGHT_NS, 2, True, False),
        ("Y", models.LIGHT_EW, 5, False, True),
        ("Y", models.LIGHT_EW, 2, False, False),
        ("Y", models.LIGHT_EW, 5, True, True),
        ("G", models.LIGHT_NS, 60, False, True),
        ("G", models.LIGHT_NS, 30, False, False),
        ("G", models.LIGHT_NS, 30, True, True),
        ("G", models.LIGHT_NS, 10, True, False),
        ("G", models.LIGHT_EW, 30, False, True),
        ("G", models.LIGHT_EW, 20, False, False),
        ("G", models.LIGHT_EW, 20, True, True),
        ("G", models.LIGHT_EW, 10, True, False),
    ),
)
def test_timeout(color, label, state_change_time, button_pressed, timeout_expected):
    light = models.Light(color, label)
    light._last_state_change = time.time() - state_change_time
    light._button_pressed = button_pressed
    assert light.timeout is timeout_expected


def test_button_press():
    light = models.Light(models.Light.COLOR_G, models.LIGHT_NS)
    assert light._button_pressed is False
    light.button_request()
    assert light.button_pressed is True
    light._color = models.Light.COLOR_Y
    light._button_pressed is False
    light.button_request()
    assert light.button_pressed is False


@pytest.mark.parametrize(
    "current, next_val",
    (
        ("G", "Y"),
        ("Y", "R"),
        ("R", "G"),
    ),
)
def test_peek_and_advance(current, next_val):
    light = models.Light(current, models.LIGHT_NS)
    assert light.peek() == next_val
    current_time = light._last_state_change
    assert light.advance() == next_val
    # This should have changed by now
    assert light._last_state_change > current_time
    assert light.button_pressed is False


@pytest.fixture()
def base_state():
    return models.State(
        light_ns=models.Light(models.Light.COLOR_R, models.LIGHT_NS),
        light_ew=models.Light(models.Light.COLOR_G, models.LIGHT_EW),
    )


def test_handle_clock_tick_no_timeouts(base_state):
    original_state = copy.deepcopy(base_state)
    result = models.handle_clock_tick(base_state)
    assert result == original_state


@pytest.mark.parametrize(
    "ew_colors,ns_colors",
    (
        (("G", "Y"), ("R", "R")),
        (("Y", "R"), ("R", "G")),
        (("R", "G"), ("Y", "R")),
        (("R", "R"), ("G", "Y")),
    ),
)
def test_handle_clock_tick_with_timeouts(ew_colors, ns_colors, base_state):
    original_state = copy.deepcopy(base_state)
    light_ns = base_state[models.LIGHT_NS]
    light_ns._color = ns_colors[0]
    light_ns._last_state_change = time.time() - 100
    light_ew = base_state[models.LIGHT_EW]
    light_ew._color = ew_colors[0]
    light_ew._last_state_change = time.time() - 100
    result = models.handle_clock_tick(base_state)
    assert result != original_state
    assert result[models.LIGHT_NS].color == ns_colors[1]
    assert result[models.LIGHT_EW].color == ew_colors[1]


def test_handle_button_press(base_state):
    light_ns = base_state[models.LIGHT_NS]
    light_ns._color = "G"
    assert light_ns.button_pressed is False
    result = models.handle_button_press(base_state, models.Model.Button_NS)
    assert result[models.LIGHT_NS].button_pressed is True

    light_ew = base_state[models.LIGHT_EW]
    light_ew._color = "G"
    assert light_ew.button_pressed is False
    result = models.handle_button_press(base_state, models.Model.Button_EW)
    assert result[models.LIGHT_EW].button_pressed is True
