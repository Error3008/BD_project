-- 1. Średni czas podróży (w godzinach) wszystkich autobusów dla każdej firmy
SELECT 
    jsonb_extract_path_text(p.data::jsonb, 'firma') AS firma,
    ROUND(
        AVG(
            EXTRACT(EPOCH FROM 
                (
                    (jsonb_extract_path_text(p.data::jsonb, 'przyjazd', 'data') || ' ' || jsonb_extract_path_text(p.data::jsonb, 'przyjazd', 'godzina'))::timestamp 
                    - 
                    (jsonb_extract_path_text(p.data::jsonb, 'odjazd', 'data') || ' ' || jsonb_extract_path_text(p.data::jsonb, 'odjazd', 'godzina'))::timestamp
                )
            ) / 3600
        ), 1
    ) AS sredni_czas_podrozy
FROM przyjazdy_i_odjazdy_json p
GROUP BY jsonb_extract_path_text(p.data::jsonb, 'firma')
ORDER BY sredni_czas_podrozy DESC;



-- 2. Statystyki dotyczące liczby pasażerów i rejsów dla każdego stanowiska odjazdu
SELECT 
    b.data->'odjazd'->>'miasto' AS odjazd_miasto,
    COUNT(DISTINCT p.data->>'id') AS liczba_rejsow,
    COUNT(b.data->>'id') AS liczba_pasazerow,
    ROUND(COUNT(b.data->>'id') * 1.0 / COUNT(DISTINCT p.data->>'id'), 2) AS srednio_pasazerow_na_rejs
FROM przyjazdy_i_odjazdy_json p
LEFT JOIN bilety_json b 
    ON p.data->>'numer_rejsu' = b.data->>'numer_rejsu'
    AND p.data->'odjazd'->>'miasto' = b.data->'odjazd'->>'miasto'
GROUP BY b.data->'odjazd'->>'miasto'
HAVING ROUND(COUNT(b.data->>'id') * 1.0 / COUNT(DISTINCT p.data->>'id'), 2) > 1.0
ORDER BY liczba_pasazerow DESC;
-- 3. Dla każdej trasy (miasto wyjazdu → miasto przyjazdu): liczba pasażerów i najpopularniejszy rejs
SELECT 
    b.data->'odjazd'->>'miasto' AS odjazd_miasto,
    b.data->'przyjazd'->>'miasto' AS przyjazd_miasto,
    b.data->>'numer_rejsu' AS numer_rejsu,
    COUNT(*) AS liczba_pasazerow
FROM bilety_json b
GROUP BY b.data->'odjazd'->>'miasto', b.data->'przyjazd'->>'miasto', b.data->>'numer_rejsu'
ORDER BY liczba_pasazerow DESC;
-- 4. Statystyki firmy: liczba rejsów, liczba pasażerów, średnia liczba pasażerów na rejs
SELECT 
    p.data->>'firma' AS firma,
    COUNT(DISTINCT p.data->>'numer_rejsu') AS liczba_rejsow,
    COUNT(b.data->>'id') AS liczba_pasazerow,
    ROUND(COUNT(b.data->>'id') * 1.0 / COUNT(DISTINCT p.data->>'numer_rejsu'), 2) AS srednio_pasazerow_na_rejs
FROM przyjazdy_i_odjazdy_json p
LEFT JOIN bilety_json b ON b.data->>'numer_rejsu' = p.data->>'numer_rejsu'
GROUP BY p.data->>'firma'
HAVING COUNT(b.data->>'id') > 0
ORDER BY srednio_pasazerow_na_rejs DESC;