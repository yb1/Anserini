package io.anserini.search;

/**
 * Created by youngbinkim on 7/27/16.
 */
public class SearchTerm {
    public SearchTerm(float score, String term) {
        this.score = score;
        this.term = term;
    }
    private float score;
    private String term;

    public float getScore() {
        return score;
    }

    public String getTerm() {
        return term;
    }
}