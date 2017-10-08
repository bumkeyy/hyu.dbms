/**
*    @class DB_2017_ITE2038
*    @file  project1.sql
*    @author Kibeom Kwon (kgbum2222@gmail.com)
*    @since 2017-10-08
*/
use Pokemon
SELECT name FROM Trainer WHERE hometown = 'Blue City';
SELECT name FROM Trainer WHERE hometown = 'Brown City' OR hometown = 'Rainbow City';
SELECT name, hometown FROM Trainer WHERE name like 'a%' OR name LIKE 'e%' OR name LIKE 'i%' OR name LIKE 'u%' OR name LIKE 'o%';
SELECT name FROM Pokemon WHERE type = 'Water';
SELECT DISTINCT type FROM Pokemon;
SELECT name FROM Pokemon ORDER BY name;
SELECT name FROM Pokemon WHERE name LIKE '%s';
SELECT P1.name FROM Pokemon P1 JOIN Pokemon P2 ON P2.name = P1.name AND P2.name LIKE '%e%' WHERE P1.name LIKE '%s';
SELECT name FROM Pokemon WHERE name LIKE 'a%' OR name LIKE 'e%' OR name LIKE 'i%' OR name LIKE 'o%' OR name LIKE 'u%';
SELECT type, COUNT(*) as '#type' FROM Pokemon GROUP BY type;
SELECT level, nickname FROM CatchedPokemon ORDER BY level DESC LIMIT 3;
SELECT AVG(level) FROM CatchedPokemon;
SELECT MAX(level) - MIN(level) as GAP FROM CatchedPokemon;
SELECT COUNT(name) FROM Pokemon WHERE name BETWEEN 'b' and 'f';
SELECT COUNT(*) FROM Pokemon P1 WHERE P1.name NOT IN (SELECT P2.name FROM Pokemon P2 WHERE P2.type = 'Fire' OR P2.type = 'Grass' OR P2.type = 'Water' OR P2.type = 'Electric');
SELECT T.name as `Trainer Name`, P.name as `Pokemon Name`, CP.nickname as Nickname FROM Trainer T, Pokemon P, CatchedPokemon CP WHERE CP.nickname LIKE '% %' AND T.id = CP.owner_id AND CP.pid = P.id;
SELECT T.name as `Trainer Name` FROM Trainer T, Pokemon P, CatchedPokemon CP WHERE P.type = 'Psychic' AND P.id = CP.pid AND CP.owner_id = T.id;
SELECT T.name, T.hometown FROM Trainer T, CatchedPokemon CP WHERE T.id = CP.owner_id GROUP BY CP.owner_id ORDER BY AVG(CP.level) DESC LIMIT 3;
SELECT T.name, COUNT(*) FROM Trainer T, CatchedPokemon CP WHERE T.id = CP.owner_id GROUP BY CP.owner_id ORDER BY COUNT(owner_id) DESC;
SELECT P.name, CP.level FROM Trainer T, CatchedPokemon CP, Gym G, Pokemon P WHERE G.city = 'Sangnok City' AND G.leader_id = T.id AND T.id = CP.owner_id AND P.id = CP.pid ORDER BY CP.level;
SELECT P.name, COUNT(CP.pid) as count FROM Pokemon P LEFT JOIN CatchedPokemon CP ON P.id = CP.pid GROUP BY P.name ORDER BY COUNT(CP.pid) DESC;
SELECT P1.name FROM Pokemon P1 WHERE P1.id IN (SELECT E1.after_id FROM Evolution E1 WHERE E1.before_id IN (SELECT E2.after_id FROM Evolution E2 WHERE E2.before_id IN (SELECT P2.id FROM Pokemon P2 WHERE P2.name = 'Charmander')));
SELECT DISTINCT P.name FROM Pokemon P, CatchedPokemon CP WHERE P.id = CP.pid AND P.id <= 30 ORDER BY P.name;
SELECT T.name, P.type FROM Trainer T, Pokemon P, CatchedPokemon CP WHERE T.id = CP.owner_id AND P.id = CP.pid GROUP BY T.name HAVING COUNT(DISTINCT P.type) = 1;
SELECT T.name, P.type, COUNT(*) as Count FROM Trainer T, Pokemon P, CatchedPokemon CP WHERE T.id = CP.owner_id AND P.id = CP.pid GROUP BY T.name, P.type;
SELECT T.name as Trainer, P.name as Pokemon, COUNT(*) as COUNT FROM Trainer T, Pokemon P, CatchedPokemon CP  WHERE T.id = CP.owner_id AND CP.pid = P.id GROUP BY T.name HAVING COUNT(DISTINCT P.name) = 1;
SELECT T1.name, G.city  FROM Trainer T1, Gym G WHERE T1.id NOT IN (SELECT T2.id FROM Trainer T2, Pokemon P2, CatchedPokemon CP2 WHERE T2.id = CP2.owner_id AND P2.id = CP2.pid GROUP BY T2.name  HAVING COUNT(DISTINCT P2.type) = 1) AND T1.id = G.leader_id;
SELECT DISTINCT T2.name, (SELECT SUM(CP1.level) FROM CatchedPokemon CP1, Trainer T1, Pokemon P1 WHERE T1.id = T2.id AND CP1.owner_id = T2.id AND P1.id = CP1.pid AND CP1.level >= 50) as level FROM Trainer T2, Gym G  WHERE G.leader_id = T2.id;
SELECT DISTINCT P1.name FROM Pokemon P1, Trainer T1, CatchedPokemon CP1 WHERE P1.id IN (SELECT P2.id FROM Pokemon P2, Trainer T2, CatchedPokemon CP2 WHERE T2.hometown = 'Blue City' AND T2.id = CP2.owner_id AND CP2.pid = P2.id) AND T1.hometown = 'Sangnok City' AND T1.id = CP1.owner_id  AND CP1.pid = P1.id;
SELECT P.name as Pokemon FROM Evolution E, Pokemon P WHERE E.after_id NOT IN (SELECT before_id FROM Evolution) AND E.after_id = P.id;



