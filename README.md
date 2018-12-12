# Traffic Data Analysis in Hefei, China

In my hometown Hefei in east China, people often drive to work at highrises, which are located in a rather small-sized city with a really high population density. It is really different from people going to work in California – driving through freeway to much more spread-out company buildings. There will also be parents driving their kids to school. All these driving causes a ton of traffic, and the traffic condition change based on different resons – driving to work, picking up kids, weather, etc.

In this project, I had a Python script I wrote at the beginning of 2018 running on a server collecting real-time traffic information from four representative locations using AMaps API. (AMap is of the largest map providers in China, like Google Maps in the US). The four locations are:
- `Zhanqian Rd`  (Near train station, doesn't have much going-to-work traffic as compared to others)
- `Huizhou Ave (Dazhonglou)` (Close to city central, one of the busiest road)
- `Ningguo Rd`   (Close to city central, some restaurants nearby)
- `Xiyou Rd`  (Near a major highschool, big shopping malls nearby)

The API gives the following data:
- expedite % (vehicles move normally)
- congestion % (vehicles move slowly)
- block % (vehicles rarely move)
- unknown % (data unknown)

Those data are saved into a csv file for later analysis use, and also published to thingspeak for a direct visualization in real time (https://thingspeak.com/channels/399882)

In the jupyter notebook Analysis.ipynb, the saved csv containing data from the beginning of the year to October is loaded using `pandas`, and after some manupulation, plotted with `seaborn`.