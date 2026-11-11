import numpy as np

from guardian_sim import engine
from guardian_sim.dealer import Dealer, DealContext, lethal_hand, random_hand
from test_engine import brute_force_solvable, midgame_board


def test_lethal_hand_is_two_fit_one_not_and_unsolvable():
    rng = np.random.default_rng(3)
    found = 0
    for seed in range(15):
        board = midgame_board(seed, target=34)
        hand = lethal_hand(board, rng)
        if hand is None:
            continue
        found += 1
        fit_now = [engine.fits(board, p) for p in hand]
        assert sum(fit_now) >= 2 and not all(fit_now)
        assert not brute_force_solvable(board, hand)
    assert found > 5


def test_lethal_hand_impossible_on_empty_board():
    assert lethal_hand(0, np.random.default_rng(0)) is None


def test_fair_random_hand_is_solvable():
    rng = np.random.default_rng(5)
    ok = 0
    for seed in range(20):
        board = midgame_board(seed, target=30)
        ok += engine.solvable(board, random_hand(rng, 10, board))
    assert ok >= 19


def _ctx(**kw):
    base = dict(board=midgame_board(1, 30), score=6000, high_score=5000, game_num=5,
                rewarded_history=[True, True, True], last_rewarded_ecpm=10.0,
                loaded_rewarded_ecpm=35.0)
    base.update(kw)
    return DealContext(**base)


def _fires(ctx, delta=1000):
    d = Dealer("predatory")
    state = d.new_game_state(ctx.high_score, np.random.default_rng(0))
    state.ad_delta, state.ad_bar = delta, ctx.high_score + delta
    return d._ad_harvest(ctx, state)


def test_ad_harvest_conditions_like_the_reference_case():
    assert _fires(_ctx())                                              # todo se cumple
    assert not _fires(_ctx(rewarded_history=[True, True]))             # solo 2 anuncios
    assert not _fires(_ctx(rewarded_history=[True, False, True]))      # uno cerrado antes
    assert not _fires(_ctx(score=5999))                                # 1 punto por debajo
    assert not _fires(_ctx(loaded_rewarded_ecpm=34.99))                # justo bajo 3,5x
    assert _fires(_ctx(score=6000, loaded_rewarded_ecpm=35.0))         # exactamente en el borde


def test_ad_harvest_fires_once_then_rearms_after_revive():
    d = Dealer("predatory")
    rng = np.random.default_rng(2)
    ctx = _ctx()
    state = d.new_game_state(ctx.high_score, rng)
    state.ad_delta, state.ad_bar = 1000, 6000
    first = d.deal(ctx, state, rng)
    assert first.kind == "lethal" and first.rule == "ad_harvest"
    assert not d._ad_harvest(ctx, state)          # no repite en la misma partida
    d.on_revive(state)
    assert state.ad_bar == 7000                   # el umbral sube otro delta
    assert d._ad_harvest(_ctx(score=7000), state)


def test_neutral_never_forces_and_block_suppresses():
    rng = np.random.default_rng(4)
    neutral = Dealer("neutral")
    pred = Dealer("predatory")
    for _ in range(50):
        ctx = _ctx()
        assert neutral.deal(ctx, neutral.new_game_state(5000, rng), rng).kind == "normal"
        state = pred.new_game_state(5000, rng)
        state.ad_bar = 6000
        blocked = pred.deal(ctx, state, rng, blocked=True)
        assert blocked.kind == "normal" and blocked.suppressed_rule is not None
