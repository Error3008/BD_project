-- 1. Średni czas podróży (w godzinach) wszystkich autobusów dla każdej firmy
SELECT firma,
    ROUND(
        AVG(
            EXTRACT( EPOCH FROM 
            ((przyjazd__data + przyjazd__godzina) 
            - (odjazd__data + odjazd__godzina))) / 3600
        ), 1) AS sredni_czas_podrozy
FROM przyjazdy_i_odjazdy
GROUP BY firma
ORDER BY sredni_czas_podrozy DESC;
-- 2. Statystyki dotyczące liczby pasażerów i rejsów dla każdego stanowiska odjazdu
SELECT 
    b.odjazd__miasto,
    COUNT(DISTINCT p.id) AS liczba_rejsow,
    COUNT(b.id) AS liczba_pasazerow,
    ROUND(COUNT(b.id) * 1.0 / COUNT(DISTINCT p.id), 2) AS srednio_pasazerow_na_rejs
FROM przyjazdy_i_odjazdy p
LEFT JOIN bilety b 
    ON p.numer_rejsu = b.numer_rejsu
    AND p.odjazd__miasto = b.odjazd__miasto
GROUP BY b.odjazd__miasto
HAVING ROUND(COUNT(b.id) * 1.0 / COUNT(DISTINCT p.id), 2) > 1.0
ORDER BY liczba_pasazerow DESC;
-- 3. Dla każdej trasy (miasto wyjazdu → miasto przyjazdu): liczba pasażerów i najpopularniejszy rejs
SELECT 
    b.odjazd__miasto,
    b.przyjazd__miasto,
    b.numer_rejsu,
    COUNT(*) AS liczba_pasazerow
FROM bilety b
GROUP BY b.odjazd__miasto, b.przyjazd__miasto, b.numer_rejsu
ORDER BY liczba_pasazerow DESC;
-- 4. Statystyki firmy: liczba rejsów, liczba pasażerów, średnia liczba pasażerów na rejs
SELECT 
    p.firma,
    COUNT(DISTINCT p.numer_rejsu) AS liczba_rejsow,
    COUNT(b.id) AS liczba_pasazerow,
    ROUND(COUNT(b.id) * 1.0 / COUNT(DISTINCT p.numer_rejsu), 2) AS srednio_pasazerow_na_rejs
FROM przyjazdy_i_odjazdy p
LEFT JOIN bilety b ON b.numer_rejsu = p.numer_rejsu
GROUP BY p.firma
HAVING COUNT(b.id) > 0
ORDER BY srednio_pasazerow_na_rejs DESC;