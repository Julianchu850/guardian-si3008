-- Consultas para el informe. En Athena, la columna dt es de tipo DATE:
-- compárenla con DATE 'AAAA-MM-DD', no con texto.
-- Para comparar los mismos jugadores en los tres mundos, filtren una semilla
-- (prefijo del jugador, por ejemplo 'p42-%').

-- 1. Tres mundos: ingreso vs. daño por jugador
SELECT policy,
       count(DISTINCT player_id)                                        AS jugadores,
       round(sum(ad_revenue_usd) / count(DISTINCT player_id), 3)       AS anuncios_usd_por_jugador,
       round(sum(ad_impressions) / (sum(play_seconds) / 3600), 1)       AS anuncios_por_hora,
       round(sum(game_overs_above_record) * 1.0 / sum(game_overs), 3)   AS derrotas_tras_record,
       round(sum(revives_accepted) * 1.0 / sum(revive_offers), 3)       AS acepta_revive
FROM guardian.gold_player_daily
WHERE dt BETWEEN DATE '2026-11-16' AND DATE '2026-11-22'
  AND player_id LIKE 'p42-%'
GROUP BY policy
ORDER BY policy;

-- 2. Manos letales por 100 partidas, por mundo y regla (verdad terreno)
WITH partidas AS (
  SELECT policy, sum(games) AS partidas
  FROM guardian.gold_player_daily
  WHERE dt BETWEEN DATE '2026-11-16' AND DATE '2026-11-22' AND player_id LIKE 'p42-%'
  GROUP BY policy
)
SELECT l.policy,
       coalesce(l.rule, l.suppressed_rule, 'none')                       AS regla,
       sum(CASE WHEN l.forced THEN 1 ELSE 0 END)                         AS forzados,
       sum(CASE WHEN l.suppressed_rule IS NOT NULL THEN 1 ELSE 0 END)    AS bloqueados_por_guardian,
       round(100.0 * sum(CASE WHEN l.forced THEN 1 ELSE 0 END) / max(p.partidas), 2) AS letales_por_100_partidas
FROM guardian.silver_deal_labels l
JOIN partidas p ON p.policy = l.policy
WHERE l.dt BETWEEN DATE '2026-11-16' AND DATE '2026-11-22' AND l.player_id LIKE 'p42-%'
GROUP BY 1, 2
ORDER BY 1, 2;

-- 3. ¿A quién golpea la manipulación? (perfil oculto del simulador)
SELECT l.policy, t.profile,
       sum(CASE WHEN l.forced THEN 1 ELSE 0 END) AS forzados,
       count(DISTINCT l.player_id)               AS jugadores
FROM guardian.silver_deal_labels l
JOIN guardian.silver_players_truth t ON t.player_id = l.player_id AND t.policy = l.policy
WHERE l.dt BETWEEN DATE '2026-11-16' AND DATE '2026-11-22' AND l.player_id LIKE 'p42-%'
GROUP BY 1, 2
ORDER BY 1, 3 DESC;

-- 4. Concentración: qué parte del ingreso aporta el 5 % de jugadores que más gasta
WITH por_jugador AS (
  SELECT policy, player_id, sum(total_revenue_usd) AS ingreso
  FROM guardian.gold_player_daily
  WHERE dt BETWEEN DATE '2026-11-16' AND DATE '2026-11-22' AND player_id LIKE 'p42-%'
  GROUP BY 1, 2
), rank AS (
  SELECT *, row_number() OVER (PARTITION BY policy ORDER BY ingreso DESC) AS rn,
         count(*) OVER (PARTITION BY policy) AS n
  FROM por_jugador
)
SELECT policy,
       round(sum(CASE WHEN rn <= greatest(1, round(n * 0.05)) THEN ingreso ELSE 0 END) / sum(ingreso), 3)
         AS participacion_top5
FROM rank GROUP BY policy ORDER BY policy;

-- 5. Intervenciones del guardián en streaming (tópico interventions; la tabla la crea el dashboard)
SELECT action, count(*) AS intervenciones, count(DISTINCT player_id) AS jugadores,
       round(avg(risk), 3) AS riesgo_medio
FROM guardian.bronze_interventions
GROUP BY action;
