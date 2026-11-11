import itertools

import numpy as np

from guardian_sim import engine
from guardian_sim.engine import COL, PIECE_BY_NAME, PLACEMENTS, ROW


def brute_force_solvable(board, hand):
    """Solver independiente, sin memoria ni atajos, para validar engine.solvable."""
    for order in itertools.permutations(hand):
        stack = [(board, 0)]
        while stack:
            b, k = stack.pop()
            if k == len(order):
                return True
            for m in PLACEMENTS[order[k]]:
                if b & m == 0:
                    nb, _ = engine.place(b, m)
                    stack.append((nb, k + 1))
    return False


def midgame_board(seed, target=30):
    rng = np.random.default_rng(seed)
    board = 0
    while engine.popcount(board) < target:
        pid = int(rng.integers(engine.N_PIECES))
        opts = engine.fitting_placements(board, pid)
        if opts:
            board, _ = engine.place(board, opts[int(rng.integers(len(opts)))])
    return board


def test_placement_counts():
    assert len(PLACEMENTS[PIECE_BY_NAME["dot"].id]) == 64
    assert len(PLACEMENTS[PIECE_BY_NAME["sq3"].id]) == 36
    assert len(PLACEMENTS[PIECE_BY_NAME["line5_h"].id]) == 32


def test_place_clears_row():
    board = ROW[0] & ~1          # fila 0 llena salvo la celda (0,0)
    nb, lines = engine.place(board, 1)
    assert lines == 1 and nb == 0


def test_place_clears_row_and_column_at_once():
    cell = 1 << (3 * 8 + 3)
    board = (ROW[3] | COL[3]) & ~cell
    nb, lines = engine.place(board, cell)
    assert lines == 2 and nb == 0


def test_features():
    assert engine.free_3x3(0) == 36
    assert engine.holes(0) == 0
    hole = 1 << (4 * 8 + 4)
    ring = 0
    for r, c in [(3, 4), (5, 4), (4, 3), (4, 5)]:
        ring |= 1 << (r * 8 + c)
    assert engine.holes(ring) == 1
    assert engine.holes(ring | hole) == 0
    assert engine.holes(engine.FULL & ~1) == 1   # esquina vacía rodeada


def test_solvable_matches_brute_force():
    rng = np.random.default_rng(0)
    for seed in range(25):
        board = midgame_board(seed, target=int(rng.integers(20, 40)))
        hand = [int(x) for x in rng.integers(engine.N_PIECES, size=3)]
        assert engine.solvable(board, hand) == brute_force_solvable(board, hand)


def test_empty_board_always_solvable():
    rng = np.random.default_rng(1)
    for _ in range(20):
        hand = [int(x) for x in rng.integers(engine.N_PIECES, size=3)]
        assert engine.solvable(0, hand)
