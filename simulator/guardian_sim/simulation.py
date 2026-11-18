"""Simulador: jugadores, sesiones y partidas sobre un reloj simulado.

Cada jugador se simula día por día. Un día genera sesiones; una sesión,
partidas; una partida, repartos de piezas. Cada paso emite eventos con el
esquema de docs/esquema_eventos.md. El reloj es simulado: una semana de juego
de cientos de jugadores se genera en minutos.

Salidas por día:
- events: lo que vería el estudio (va a Kafka / S3 bronce). Solo variables
  observables.
- labels: verdad terreno de cada reparto (si fue forzado y por qué regla).
  No viaja por el stream: se guarda aparte para entrenar y evaluar.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

import numpy as np

from . import ads, engine
from .agent import Brain
from .dealer import Dealer, DealContext
from .profiles import Traits, sample_profile, sample_traits

SCHEMA_VERSION = 1
MAX_REVIVES = {"neutral": 1, "predatory": 2, "guarded": 2}
# "guarded" = el mismo juego depredador, con el guardián activo (tercer mundo).
DEALER_POLICY = {"neutral": "neutral", "predatory": "predatory", "guarded": "predatory"}
PREDATORY = {"predatory", "guarded"}
MAX_DEALS_PER_GAME = 400
MAX_GAMES_PER_SESSION = 40
INTERSTITIAL_EVERY = 3            # un intersticial cada N partidas (ambas políticas)
OFFERS = [("revive_pack", 0.99), ("no_ads_week", 2.99), ("gem_bundle", 4.99),
          ("mega_bundle", 19.99)]
BASE_OFFER_PROB = 0.03            # oferta "normal" de la tienda
PREDATORY_OFFER_PROB = 0.35       # oferta tras una derrota frustrante (política A)
CHURN_AFTER_ABSENT_DAYS = 3


class Interventions:
    """Interfaz para las acciones del guardián. Por defecto no hace nada.

    Acciones: "block_manipulation" (el dealer no aplica reglas depredadoras),
    "suggest_break" (se sugiere terminar la sesión), "spend_limit" (sin compras).
    """

    def for_player(self, player: "Player") -> "Interventions":
        return self

    def observe(self, event: dict) -> None:
        pass

    def pre_deal(self, game_id: str, score: int, high_score: int, loaded_ecpm: float,
                 deal_num: int, board_hex: str) -> tuple[bool, float | None]:
        return False, None

    def active(self, player_id: str, ts: float) -> set[str]:
        return set()


class GuardianInterventions(Interventions):
    """Guardián embebido: el mismo modelo y variables que el servicio de Kafka,
    pero decidiendo ANTES de cada reparto (como lo haría el backend del juego).
    El estado de cada jugador viaja en Player.guard_state entre días y procesos."""

    def __init__(self, model_uri: str, threshold: float | None = None):
        self.model_uri = model_uri
        self.threshold = threshold
        self._guardian = None
        self._st = None
        self._pid = None

    def _g(self):
        if self._guardian is None:
            from guardian_ml.guardian import Guardian
            from guardian_ml.model import RiskModel
            self._guardian = Guardian(RiskModel.load(self.model_uri), self.threshold)
        return self._guardian

    def for_player(self, player: "Player") -> "GuardianInterventions":
        from guardian_ml.guardian import GuardState
        if player.guard_state is None:
            player.guard_state = GuardState()
        bound = GuardianInterventions(self.model_uri, self.threshold)
        bound._guardian, bound._st, bound._pid = self._g(), player.guard_state, player.player_id
        return bound

    def observe(self, event: dict) -> None:
        self._g().observe(event, self._st, score_deals=False)

    def pre_deal(self, game_id, score, high_score, loaded_ecpm, deal_num, board_hex):
        return self._g().pre_deal(self._st, game_id, score, high_score, loaded_ecpm,
                                  deal_num, board_hex)

    def active(self, player_id: str, ts: float) -> set[str]:
        return set(self._st.session_actions) if self._st else set()


@dataclass
class Player:
    player_id: str
    profile: str
    traits: Traits
    policy: str
    country: str
    platform: str
    device_tier: str
    install_day: int
    rng: np.random.Generator
    high_score: int = 0
    games_played: int = 0
    sessions_played: int = 0
    rewarded_history: list[bool] = field(default_factory=list)
    last_rewarded_ecpm: float = 0.0
    frustration: float = 0.0
    spent_usd: float = 0.0
    days_absent: int = 0
    unfair_yesterday: int = 0
    churned: bool = False
    event_seq: int = 0
    guard_state: object = None    # estado del guardián embebido (solo política guarded)

    def observable(self, install_ts: float) -> dict:
        return {"player_id": self.player_id, "country": self.country,
                "platform": self.platform, "device_tier": self.device_tier,
                "install_ts": iso(install_ts), "policy": self.policy}

    def truth(self) -> dict:
        return {"player_id": self.player_id, "profile": self.profile,
                "policy": self.policy, **self.traits.as_dict()}


def iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def create_players(n: int, policy: str, seed: int, days: int) -> list[Player]:
    """policy: neutral | predatory | split (asignación A/B al azar por jugador)."""
    master = np.random.default_rng(seed)
    offsets: dict[str, int] = {}
    counts: dict[str, int] = {}
    players = []
    for i in range(n):
        rng = np.random.default_rng([seed, i])
        prof = sample_profile(rng)
        pol = policy
        if policy == "split":  # A/B estratificado: mitad de cada perfil en cada política
            offsets.setdefault(prof, int(master.integers(0, 2)))
            counts[prof] = counts.get(prof, 0) + 1
            pol = "predatory" if (counts[prof] + offsets[prof]) % 2 else "neutral"
        players.append(Player(
            player_id=f"p{seed}-{i:05d}", profile=prof, traits=sample_traits(prof, rng), policy=pol,
            country=ads.sample_choice(rng, ads.COUNTRY_SHARE),
            platform=ads.sample_choice(rng, ads.PLATFORM_SHARE),
            device_tier=ads.sample_choice(rng, ads.DEVICE_SHARE),
            install_day=int(rng.integers(0, min(3, days))), rng=rng))
    return players


class _DayRun:
    """Simula un día de un jugador y acumula sus eventos y etiquetas."""

    def __init__(self, player: Player, day_start: float, interventions: Interventions):
        self.p = player
        self.rng = player.rng
        self.brain = Brain(player.traits)
        self.dealer = Dealer(DEALER_POLICY[player.policy])
        self.slots = ads.AdSlots(ads.base_ecpm(player.country, player.platform, player.device_tier))
        self.day_start = day_start
        self.iv = interventions
        self.events: list[dict] = []
        self.labels: list[dict] = []
        self.unfair_today = 0
        self.clock = day_start
        self.session_id = ""
        self.game_id: str | None = None

    # -- utilidades -----------------------------------------------------
    def emit(self, event_type: str, **payload) -> None:
        self.p.event_seq += 1
        self.events.append({
            "event_id": f"{self.p.player_id}-{self.p.event_seq}",
            "event_type": event_type, "ts": self.clock,
            "player_id": self.p.player_id, "session_id": self.session_id,
            "game_id": self.game_id, "policy": self.p.policy, "source": "sim",
            "schema_version": SCHEMA_VERSION, **payload})
        self.iv.observe(self.events[-1])

    def tick(self, seconds: float) -> None:
        self.clock += seconds

    # -- día ------------------------------------------------------------
    def run(self) -> None:
        t = self.p.traits
        n_sessions = max(1, int(self.rng.poisson(t.sessions_per_day)))
        starts = sorted(self._session_start_hour() for _ in range(n_sessions))
        for h in starts:
            start = self.day_start + h * 3600
            self.clock = max(self.clock, start)
            self.play_session()
            self.tick(float(self.rng.uniform(600, 3600)))  # pausa mínima entre sesiones
            if self.clock > self.day_start + 30 * 3600:
                break
        self.p.frustration *= 0.6

    def _session_start_hour(self) -> float:
        if self.rng.random() < self.p.traits.night_owl:
            return float(self.rng.uniform(21.5, 26.0))      # de 21:30 a 02:00
        return float(np.clip(self.rng.normal(15.5, 3.5), 7.0, 21.5))

    def play_session(self) -> None:
        p = self.p
        p.sessions_played += 1
        self.session_id = f"{p.player_id}-s{p.sessions_played}"
        self.game_id = None
        self.slots.load_all(self.rng)
        session_start = self.clock
        self.emit("session_start", country=p.country, platform=p.platform,
                  device_tier=p.device_tier, local_hour=int((self.clock - self.day_start) // 3600) % 24)
        games = 0
        while games < MAX_GAMES_PER_SESSION:
            new_record, lost_near_record = self.play_game()
            games += 1
            self.maybe_bonus_ad()
            self.game_id = None
            if games % INTERSTITIAL_EVERY == 0:
                self.show_ad("interstitial", "between_games")
            active = self.iv.active(p.player_id, self.clock)
            if "suggest_break" in active and self.rng.random() < 0.7 * (1 - p.traits.chase):
                self.emit("intervention_applied", action="suggest_break")
                break
            if not self.brain.continue_session(games, p.frustration, lost_near_record,
                                               new_record, self.rng):
                break
            self.tick(float(self.rng.uniform(3, 15)))
        self.emit("session_end", games=games, duration_s=round(self.clock - session_start, 1))

    # -- partida --------------------------------------------------------
    def play_game(self) -> tuple[bool, bool]:
        p, rng = self.p, self.rng
        self.game_id = f"{p.player_id}-g{p.games_played + 1}"
        state = self.dealer.new_game_state(p.high_score, rng)
        board, score, combo, deals, revives = 0, 0, 0, 0, 0
        recent_secs: list[float] = []
        self.emit("game_start", game_num=p.games_played + 1, high_score=p.high_score)
        lost_near_record = False
        guard_announced = False

        while deals < MAX_DEALS_PER_GAME:
            self.slots.maybe_refresh(rng)
            ctx = DealContext(board=board, score=score, high_score=p.high_score,
                              game_num=p.games_played, rewarded_history=p.rewarded_history,
                              last_rewarded_ecpm=p.last_rewarded_ecpm,
                              loaded_rewarded_ecpm=self.slots.rewarded_ecpm,
                              recent_turn_secs=recent_secs, deal_num=deals)
            blocked = "block_manipulation" in self.iv.active(p.player_id, self.clock)
            guard, risk = self.iv.pre_deal(self.game_id, score, p.high_score,
                                           self.slots.rewarded_ecpm, deals + 1,
                                           engine.board_hex(board))
            if guard and not blocked and not guard_announced:
                guard_announced = True
                self.emit("intervention_applied", action="block_manipulation",
                          risk=round(risk, 5))
            blocked = blocked or guard
            deal = self.dealer.deal(ctx, state, rng, blocked=blocked)
            deals += 1
            deal_id = f"{self.game_id}-d{deals}"
            placeable = sum(engine.fits(board, pid) for pid in deal.pieces)
            self.emit("piece_deal", deal_id=deal_id, deal_num=deals, pieces=deal.pieces,
                      board=engine.board_hex(board), placeable_now=placeable, score=score,
                      high_score=p.high_score, rewarded_ecpm_loaded=self.slots.rewarded_ecpm)
            self.labels.append({
                "deal_id": deal_id, "player_id": p.player_id, "game_id": self.game_id,
                "ts": self.clock, "policy": p.policy, "kind": deal.kind, "rule": deal.rule,
                "suppressed_rule": deal.suppressed_rule, "forced": deal.kind == "lethal",
                "guardian_risk": None if risk is None else round(risk, 5),
                "solvable": engine.solvable(board, deal.pieces)})

            # El agente juega la mano.
            hand = list(deal.pieces)
            placed, lines_total, points, turn_secs = 0, 0, 0, 0.0
            for _ in range(3):
                mv = self.brain.choose_move(board, hand, rng, p.frustration)
                if mv is None:
                    break
                i, mask = mv
                pid = hand.pop(i)
                board, lines = engine.place(board, mask)
                if lines:
                    combo += 1
                points += engine.score_move(engine.PIECES[pid].size, lines, combo)
                lines_total += lines
                placed += 1
                turn_secs += self.brain.move_seconds(rng, p.frustration)
            if lines_total == 0:
                combo = 0
            score += points
            self.tick(turn_secs)
            recent_secs.append(turn_secs)
            self.emit("turn_result", deal_id=deal_id, pieces_placed=placed,
                      lines_cleared=lines_total, points=points, score=score,
                      turn_ms=int(turn_secs * 1000))
            if not hand:
                continue

            # Se quedó sin jugadas: fin de partida, salvo que reviva.
            near = p.high_score > 0 and score >= 0.9 * p.high_score
            self.emit("game_over", deal_id=deal_id, score=score, high_score=p.high_score,
                      above_record=score > p.high_score)
            if near:
                lost_near_record = True
            if revives < MAX_REVIVES[p.policy] and self.offer_revive(score, revives):
                revives += 1
                self.dealer.on_revive(state)
                board = _revive_board(board)
                self.emit("revive", revives_used=revives, board=engine.board_hex(board))
                continue
            break

        self.p.games_played += 1
        new_record = score > p.high_score
        self._after_game(new_record, lost_near_record)
        self.emit("game_end", final_score=score, high_score=p.high_score, new_record=new_record,
                  deals=deals, revives_used=revives)
        if new_record:
            p.high_score = score
        return new_record, lost_near_record and not new_record

    def maybe_bonus_ad(self) -> None:
        """Al final de la partida se ofrece un rewarded opcional (monedas extra)."""
        if self.rng.random() < 0.5:
            accepted = self.rng.random() < 0.15 + 0.5 * self.p.traits.impulsivity
            self.emit("reward_offer", placement="bonus_coins", accepted=accepted,
                      rewarded_ecpm_loaded=self.slots.rewarded_ecpm)
            if accepted:
                self.show_ad("rewarded", "bonus_coins")

    def offer_revive(self, score: int, revives: int) -> bool:
        p = self.p
        self.emit("revive_offer", revives_used=revives,
                  rewarded_ecpm_loaded=self.slots.rewarded_ecpm)
        accepted = self.brain.accept_revive(score, p.high_score, revives, p.frustration, self.rng)
        self.emit("revive_response", accepted=accepted)
        if not accepted:
            return False
        return self.show_ad("rewarded", "revive")

    def show_ad(self, ad_type: str, placement: str) -> bool:
        p = self.p
        ecpm = self.slots.rewarded_ecpm if ad_type == "rewarded" else self.slots.interstitial_ecpm
        completed, secs = self.brain.watch_ad(ad_type, self.rng)
        self.tick(secs)
        self.emit("ad_impression", ad_type=ad_type, placement=placement, ecpm=ecpm,
                  revenue_usd=round(ecpm / 1000.0, 6), completed=completed,
                  watch_ms=int(secs * 1000))
        if ad_type == "rewarded":
            p.rewarded_history.insert(0, completed)
            del p.rewarded_history[10:]
            p.last_rewarded_ecpm = ecpm
            self.slots.load_rewarded(self.rng)
        else:
            self.slots.load_interstitial(self.rng)
        return completed

    def _after_game(self, new_record: bool, lost_near_record: bool) -> None:
        p, rng = self.p, self.rng
        if new_record:
            p.frustration = max(0.0, p.frustration - 0.5)
        else:
            p.frustration += 1.0 + (1.5 if lost_near_record else 0.0)
            if lost_near_record:
                self.unfair_today += 1
        # Ofertas de la tienda.
        prob = BASE_OFFER_PROB
        if p.policy in PREDATORY and lost_near_record and not new_record:
            prob = PREDATORY_OFFER_PROB
        if rng.random() < prob:
            k = min(len(OFFERS) - 1, int(rng.integers(0, 2) + 2 * p.traits.spend * 2))
            offer_id, price = OFFERS[k]
            self.emit("store_offer", offer_id=offer_id, price_usd=price,
                      trigger="post_loss" if prob == PREDATORY_OFFER_PROB else "store")
            blocked = "spend_limit" in self.iv.active(p.player_id, self.clock)
            if not blocked and self.brain.buy(price, p.spent_usd, p.frustration, rng):
                p.spent_usd += price
                self.emit("purchase", offer_id=offer_id, price_usd=price)


def _revive_board(board: int) -> int:
    """Revivir limpia las 2 filas y las 2 columnas más llenas."""
    rows = sorted(range(8), key=lambda r: -engine.popcount(board & engine.ROW[r]))[:2]
    cols = sorted(range(8), key=lambda c: -engine.popcount(board & engine.COL[c]))[:2]
    for r in rows:
        board &= ~engine.ROW[r]
    for c in cols:
        board &= ~engine.COL[c]
    return board & engine.FULL


def simulate_player_day(player: Player, day: int, day_start: float,
                        interventions: Interventions | None = None) -> tuple[Player, list, list]:
    """Simula un día de un jugador. Devuelve (jugador actualizado, eventos, etiquetas)."""
    if player.churned or day < player.install_day:
        return player, [], []
    if day > player.install_day:
        brain = Brain(player.traits)
        if not brain.return_tomorrow(player.unfair_yesterday, player.rng):
            player.days_absent += 1
            player.unfair_yesterday = 0
            if player.days_absent >= CHURN_AFTER_ABSENT_DAYS:
                player.churned = True
            return player, [], []
    player.days_absent = 0
    run = _DayRun(player, day_start, (interventions or Interventions()).for_player(player))
    run.run()
    player.unfair_yesterday = run.unfair_today
    return player, run.events, run.labels


def day_start_ts(start_date: str, day: int) -> float:
    base = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc).timestamp()
    return base + day * 86400.0

