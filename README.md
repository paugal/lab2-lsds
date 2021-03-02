## Exercise 3
- To run locally  
spark-submit --class upf.edu.TwitterLanguageFilterApp --master local[2] target/lab2-1.0-SNAPSHOT.jar es ./output/run1 ./input
/
- To run in emr  
  --class upf.edu.TwitterLanguageFilterApp s3://edu.upf.ldsd2021.lab2.grp201.team8/jars/lab2-1.0-SNAPSHOT.jar es s3://edu.upf.ldsd2021.lab2.grp201.team8/output/run2 s3://edu.upf.ldsd2021.lab2.grp201.team8/input/  
  

RESULTS:  
Time to process tweets in es is 76766 ms  
Time to process tweets in en is 80622 ms  
Time to process tweets in fr is 75381 ms

## Exercise 4
- To run locally  
  spark-submit --class upf.edu.BiGramsApp --master local[2] target/lab2-1.0-SNAPSHOT.jar es ./output/run2 ./input
  /
- To run in emr  
  --class upf.edu.BiGramsApp s3://edu.upf.ldsd2021.lab2.grp201.team8/jars/lab2-1.0-SNAPSHOT.jar es s3://edu.upf.ldsd2021.lab2.grp201.team8/output/run2 s3://edu.upf.ldsd2021.lab2.grp201.team8/input/
  
RESULTS:  
(139,(el,a�o))  
(123,(lo,que))  
(115,(a�o,que))  
(114,(de,la))  
(112,(que,viene))  
(106,(de,eurovision))  
(103,(la,canci�n))  
(99,(que,no))  
(84,(en,el))  
(82,(ha,ganado))  

## Exercise 5
- To run locally  
  spark-submit --class upf.edu.MostRetweetedApp --master local[2] target/lab2-1.0-SNAPSHOT.jar es ./output/run3 ./input
  /
- To run in emr  
  --class upf.edu.MostRetweetedApp s3://edu.upf.ldsd2021.lab2.grp201.team8/jars/lab2-1.0-SNAPSHOT.jar es s3://edu.upf.ldsd2021.lab2.grp201.team8/output/run3 s3://edu.upf.ldsd2021.lab2.grp201.team8/input/

RESULTS:  (in part-00000)  
(851,(Manel Navarro Music,Así que el año pasado quedo último con un gallo y este año gana una gallina... #Eurovision https://t.co/EfvXQbb8jp))  
(500,(yopor,cuando apago la luz del pasillo para irme a mi habitación y que no me maten los espíritus #Eurovision https://t.co/0naU3Xmt44))  
(469,(Manu Guix,Para mi @Amaia_ot2017 y @Alfred_ot2017 merecían mucho más. Siempre les admiraré por su valentía, su manera de ser,… https://t.co/iI81kdETE3))  
(387,(Ibai,Rodolfo Chikilicuatre, un actor disfrazado con una guitarra de plástico quedó siete puestos por encima que la ganad… https://t.co/dFuBbH3PDz))  
(342,(Alexelcapo,Cuando Israel gana eurovision con una canción contra el bullying pero tu país decide bombardear Gaza por los jajas https://t.co/iH3vnZ8Lef))  
(271,(El Jueves,La canción ganadora va sobre el drama del bullying. Encomiable autocrítica, viendo cómo trata Israel a Palestina #Eurovision))  
(235,(Eurovision,Here it is, the final results for #Eurovision Song Contest 2018! #ESC2018 #AllAboard https://t.co/FcXZVR6zod))  
(166,(Paquita Salas,Qué guasa tiene la niña, ¿eh? #Eurovision https://t.co/Iv1yottkvQ))  
(71,(BBC Eurovision🇬🇧,See what he did there? #Eurovision #CzechRepublic  #CZE https://t.co/DwdfXmTqXg))  
(67,(eurovision_rtve,Alfred dice que se sienten ganadores, ¡mira porqué! #Eurovision 👉  https://t.co/d5qvJy1kSi https://t.co/YgEIipAvzj))  
 