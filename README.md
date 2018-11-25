# A Movie behind a Script

## Abstract

A movie is a recording of moving images that tell a story and that people watch on a screen or television. People devote money and attention to what they find entertaining. From a historical point of view, works of art have acted as a reflection of our society and our culture. We have come a long way since the public screening of ten of Lumière brothers' short films in Paris on 28 December 1895 to Avatar, a movie whose cost of development reached more than 200 million dollars and recorded nearly 2 billion dollars in gross, the highest of all-time. During all this time, scripts have remained the base stone to a movie. What script must one write for a movie to prosper? We have data spanning nearly 100 years with over 30 million movies to try to solve this puzzle. Using the [OpenSubtitles](https://icitdocs.epfl.ch/display/clusterdocs/OpenSubtitles) dataset and the [IMDb](https://datasets.imdbws.com/) dataset to analyze movies' scripts and measure their popularity we hope to provide a better insight to what makes a good and a bad movie.

[//]: # (A 150 word description of the project idea, goals, dataset used. What story you would like to tell and why? What's the motivation behind your project?)

## Research questions

- How does the complexity of words employed by a movie affect its popularity?
- How does the spread of words in a movie affect its popularity?
- How has the vocabulary used in movies' scripts evolved across the years?
- Can we predict the popularity of a movie based on its script ?

[//]: # (A list of research questions you would like to address during the project.)

## Dataset

- [OpenSubtitles](https://icitdocs.epfl.ch/display/clusterdocs/OpenSubtitles): consists of 3.74 million subtitle files over 62 languages and covers a total of 152,939 movies or TV episodes. The size of the dataset is 31GB and is provided in XML and TXT format.

- [IMDb Datasets](https://datasets.imdbws.com/): contain the information of movies and shows, cast, actors, directors and writers, TV episodes and ratings and votes for each title. Each dataset is contained in a gzipped, tab-separated-values (TSV) formatted file in the UTF-8 character set.

Each movie/show in the OpenSubtitles dataset is determined by its IMDb identifier allowing us to enrich the OpenSubtitles dataset with the IMDb dataset.

Subtitle files contain for each subtitle a unique number of the subtitle shown, timestamps for the duration the subtitle is shown and the text displayed.

An extract of a subtitle file:
```
3
00:00:39,299 --> 00:00:41,099
Sir, we're getting
a distress call

4
00:00:41,168 --> 00:00:42,634
from a civilian aircraft.

5
00:00:46,540 --> 00:00:49,641
CIC visually confirms
a Cessna 172.
```

We intend to count the frequency of words, the number of distinct words in a movie and compute the average length of sentences to determine the complexity of a movie's script.

[//]: # (List the datasets you want to use, and some ideas on how do you expect to get, manage, process and enrich it/them. Show us you've read the docs and some examples, and you've a clear idea on what to expect. Discuss data size and format if relevant.)

## A list of internal milestones up until project milestone 2

04.11

- Set up the Git repository and project skeleton.

11.11
- Find the most convenient way to store and look up our data.
- Download the data.
- Clean our dataset.
- Develop methods to analyze the subtitles: counting words, spread of  words, mean length of sentences, dialogue time.

18.11

- Test our methods.
- Gather results.
- Start analysis.

25.11

- Set up our goals and plans for the next milestone.

[//]: # (Add here a sketch of your planning for the next project milestone.)

## Questions for TAs

- Is there a good library/way to analyze text?
- Are we allowed other programming languages and/or tools than Python? NVivo for instance.

[//]: # (Add here some questions you have for us, in general or project-specific.)
