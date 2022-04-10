from raft.models import EventType

RUNTIME_EVENTS = set(
    (
        EventType.DEBUG_REQUEST,
        EventType.ResetElectionTimeout,
        EventType.ConversionToFollower,
        EventType.ConversionToLeader,
        EventType.StartHeartbeat,
    )
)


class BaseRuntime:
    pass


class BaseEventController:
    pass
