"""Motor del juego tipo Block Blast sobre un bitboard de 8x8.

El tablero es un entero de 64 bits: el bit (fila * 8 + columna) vale 1 si la
celda está ocupada. Todas las operaciones (colocar, limpiar líneas, medir
huecos o espacio libre) son operaciones de bits, lo que permite simular miles
de partidas en Python puro.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

SIZE = 8
FULL = (1 << 64) - 1
ROW = [0xFF << (8 * r) for r in range(SIZE)]
COL = [sum(1 << (8 * r + c) for r in range(SIZE)) for c in range(SIZE)]
LINES = ROW + COL


def _bit(r: int, c: int) -> int:
    return 1 << (r * SIZE + c)


# --------------------------------------------------------------------------
# Catálogo de piezas
# --------------------------------------------------------------------------
_SHAPES: dict[str, str] = {
    "dot": "X",
    "dom_h": "XX",
    "dom_v": "X/X",
    "tri_h": "XXX",
    "tri_v": "X/X/X",
    "line4_h": "XXXX",
    "line4_v": "X/X/X/X",
    "line5_h": "XXXXX",
    "line5_v": "X/X/X/X/X",
    "sq2": "XX/XX",
    "sq3": "XXX/XXX/XXX",
    "rect23": "XXX/XXX",
    "rect32": "XX/XX/XX",
    "corner_a": "XX/X.",
    "corner_b": "XX/.X",
    "corner_c": "X./XX",
    "corner_d": ".X/XX",
    "bigL_a": "XXX/X../X..",
    "bigL_b": "XXX/..X/..X",
    "bigL_c": "X../X../XXX",
    "bigL_d": "..X/..X/XXX",
    "T_down": "XXX/.X.",
    "T_up": ".X./XXX",
    "T_right": "X./XX/X.",
    "T_left": ".X/XX/.X",
    "S_h": ".XX/XX.",
    "Z_h": "XX./.XX",
    "S_v": "X./XX/.X",
    "Z_v": ".X/XX/X.",
    "diag2": "X./.X",
}

# Frecuencia relativa con la que el repartidor neutral entrega cada pieza.
_WEIGHTS: dict[str, float] = {
    "dot": 0.5, "dom_h": 1.0, "dom_v": 1.0, "tri_h": 1.0, "tri_v": 1.0,
    "line4_h": 0.8, "line4_v": 0.8, "line5_h": 0.5, "line5_v": 0.5,
    "sq2": 1.2, "sq3": 0.6, "rect23": 0.7, "rect32": 0.7,
    "corner_a": 0.9, "corner_b": 0.9, "corner_c": 0.9, "corner_d": 0.9,
    "bigL_a": 0.5, "bigL_b": 0.5, "bigL_c": 0.5, "bigL_d": 0.5,
    "T_down": 0.7, "T_up": 0.7, "T_right": 0.7, "T_left": 0.7,
    "S_h": 0.5, "Z_h": 0.5, "S_v": 0.5, "Z_v": 0.5, "diag2": 0.3,
}


@dataclass(frozen=True)
class Piece:
    id: int
    name: str
    cells: tuple[tuple[int, int], ...]
    height: int
    width: int
    weight: float

    @property
    def size(self) -> int:
        return len(self.cells)


def _parse(pid: int, name: str, art: str) -> Piece:
    rows = art.split("/")
    cells = tuple((r, c) for r, row in enumerate(rows) for c, ch in enumerate(row) if ch == "X")
    return Piece(pid, name, cells, len(rows), max(len(r) for r in rows), _WEIGHTS[name])


PIECES: list[Piece] = [_parse(i, n, a) for i, (n, a) in enumerate(_SHAPES.items())]
PIECE_BY_NAME = {p.name: p for p in PIECES}
N_PIECES = len(PIECES)
LARGE_PIECES = [p.id for p in PIECES if p.size >= 5]


def _placements(p: Piece) -> tuple[int, ...]:
    out = []
    for r in range(SIZE - p.height + 1):
        for c in range(SIZE - p.width + 1):
            m = 0
            for dr, dc in p.cells:
                m |= _bit(r + dr, c + dc)
            out.append(m)
    return tuple(out)


# PLACEMENTS[pid] = máscaras de todas las posiciones posibles de la pieza.
PLACEMENTS: list[tuple[int, ...]] = [_placements(p) for p in PIECES]


# --------------------------------------------------------------------------
# Operaciones sobre el tablero
# --------------------------------------------------------------------------
def popcount(x: int) -> int:
    return x.bit_count()


def place(board: int, mask: int) -> tuple[int, int]:
    """Coloca una máscara ya validada y limpia filas/columnas llenas.

    Devuelve (tablero nuevo, número de líneas limpiadas).
    """
    b = board | mask
    clear = 0
    lines = 0
    for line in LINES:
        if b & line == line:
            clear |= line
            lines += 1
    return b & ~clear & FULL, lines


def fitting_placements(board: int, pid: int) -> list[int]:
    return [m for m in PLACEMENTS[pid] if board & m == 0]


def fits(board: int, pid: int) -> bool:
    for m in PLACEMENTS[pid]:
        if board & m == 0:
            return True
    return False


@lru_cache(maxsize=500_000)
def _solve(board: int, pieces: tuple[int, ...]) -> bool:
    if not pieces:
        return True
    if len(pieces) == 1:
        return fits(board, pieces[0])
    tried = set()
    for i, pid in enumerate(pieces):
        if pid in tried:
            continue
        tried.add(pid)
        rest = pieces[:i] + pieces[i + 1:]
        for m in PLACEMENTS[pid]:
            if board & m == 0:
                nb, _ = place(board, m)
                if _solve(nb, rest):
                    return True
    return False


def solvable(board: int, pieces) -> bool:
    """¿Existe algún orden y posiciones para colocar todas las piezas?

    Búsqueda exhaustiva que tiene en cuenta las líneas que se limpian entre
    una colocación y la siguiente (igual que el generador del caso real).
    """
    return _solve(board, tuple(sorted(pieces)))


# --------------------------------------------------------------------------
# Características del tablero (las usa la heurística de los agentes)
# --------------------------------------------------------------------------
_NOT_COL0 = FULL & ~COL[0]
_NOT_COL7 = FULL & ~COL[7]
_NOT_ROW7 = FULL & ~ROW[7]
# Inicios válidos de un bloque 3x3 (columna <= 5 y fila <= 5).
_START3 = sum(_bit(r, c) for r in range(6) for c in range(6))


def holes(board: int) -> int:
    """Celdas vacías rodeadas por los 4 lados (bordes cuentan como pared)."""
    empty = ~board & FULL
    left = ((board << 1) & _NOT_COL0 & FULL) | COL[0]
    right = ((board >> 1) & _NOT_COL7) | COL[7]
    up = ((board << 8) & FULL) | ROW[0]
    down = (board >> 8) | ROW[7]
    return popcount(empty & left & right & up & down)


def fragmentation(board: int) -> int:
    """Transiciones ocupado/vacío entre celdas vecinas (filas y columnas)."""
    h = (board ^ (board >> 1)) & _NOT_COL7
    v = (board ^ (board >> 8)) & _NOT_ROW7
    return popcount(h) + popcount(v)


def free_3x3(board: int) -> int:
    """Número de posiciones donde todavía cabe un bloque 3x3."""
    e = ~board & FULL
    h3 = e & (e >> 1) & (e >> 2)
    v3 = h3 & (h3 >> 8) & (h3 >> 16)
    return popcount(v3 & _START3)


# --------------------------------------------------------------------------
# Puntaje
# --------------------------------------------------------------------------
POINTS_PER_CELL = 10
POINTS_PER_LINE = 100


def score_move(piece_size: int, lines: int, combo: int) -> int:
    """Puntos de una colocación. combo = turnos seguidos limpiando líneas."""
    pts = piece_size * POINTS_PER_CELL
    if lines:
        pts += POINTS_PER_LINE * lines * lines * (1 + min(combo, 5) // 2)
    return pts


def board_hex(board: int) -> str:
    return f"{board:016x}"


def render(board: int) -> str:
    return "\n".join(
        "".join("#" if board & _bit(r, c) else "." for c in range(SIZE)) for r in range(SIZE)
    )
