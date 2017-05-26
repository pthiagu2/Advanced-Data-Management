For the system to run, both these files need to executed simultaneously.

python scraper.py <PORT>
python analyzer.py <PORT>

(port can be any currently unused localhost port, e.g. 9005)
The analyzer code has the twitter API credentials that have to set up at the top of the file for streaming data from twitter. Set up your twitter API credentials and fill in the keys.

The word2vec model has to be generated in advance using :
python word2vec.py <corpus-file-name> (the corpus file must have documents in separate lines, as in brown.txt)

(We have a pretrained word2vec model based on the brown corpus, if you want to train on a different corpus execute the above and then rerun the scraper and analyzer)