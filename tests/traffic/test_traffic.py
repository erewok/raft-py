import time

from hypothesis import given, strategies as st
from raft.traffic import models
from raft.traffic import traffic


TIME_INTERVALS = [
    2,
    5,
    10,
    20,
    30,
    45,
    60,
    90,
    100,
    200,
]
MODELS = [
    models.Model.Time,
    models.Model.Time,
    models.Model.Time,
    models.Model.Time,
    models.Model.Time,
    models.Model.Button_NS,
    models.Model.Button_EW,
]


def build_light(color, light, time_interval, boolean):
    light_inst = models.Light(color, light)
    light_inst._button_pressed = boolean
    light_inst._last_state_change = time.time() - time_interval
    return light_inst


LightStrategy = st.builds(
    build_light,
    st.sampled_from(models.Light.COLORS),
    st.sampled_from(models.LIGHTS),
    st.sampled_from(TIME_INTERVALS),
    st.booleans(),
)


@given(LightStrategy, LightStrategy, st.sampled_from(MODELS), st.sampled_from(MODELS))
def test_handle_state_always_returns_valid_state(
    light_inst1, light_inst2, event1, event2
):
    state = models.State(
        light_ns=light_inst1,
        light_ew=light_inst2,
    )
    for _ in range(65):
        state = traffic.next_state(state, event1)
        assert state.is_valid
        assert state.light_ew.color != state.light_ns.color
        state = traffic.next_state(state, event2)
        assert state.is_valid
        assert state.light_ew.color != state.light_ns.color


if __name__ == "__main__":  # type: ignore
    test_handle_state_always_returns_valid_state()
