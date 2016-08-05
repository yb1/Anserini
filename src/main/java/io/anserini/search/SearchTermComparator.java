package io.anserini.search;

import java.util.Comparator;

/**
 * Created by youngbinkim on 7/27/16.
 */
public class SearchTermComparator implements Comparator<SearchTerm> {
    public int compare(SearchTerm a, SearchTerm b) {
        return (int) (a.getScore() - b.getScore());
    }
}

