import json
import os

STATE_FILE = 'state.json'


def save_state(offset):
    state = {'offset': offset}
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            return state.get('offset', 0)
    return 0
