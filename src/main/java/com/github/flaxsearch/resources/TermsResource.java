package com.github.flaxsearch.resources;
/*
 *   Copyright (c) 2016 Lemur Consulting Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import com.github.flaxsearch.api.TermData;
import com.github.flaxsearch.api.TermsData;
import com.github.flaxsearch.util.BytesRefUtils;
import com.github.flaxsearch.util.ReaderManager;
import one.util.streamex.StreamEx;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.RegExp;

@Path("/terms/{field}")
@Produces(MediaType.APPLICATION_JSON)
public class TermsResource {

    private final ReaderManager readerManager;

    public TermsResource(ReaderManager readerManager) {
        this.readerManager = readerManager;
    }

    @GET
    public TermsData getTerms(@QueryParam("segment") Integer segment,
                                 @PathParam("field") String field,
                                 @QueryParam("from") String startTerm,
                                 @QueryParam("filter") String filter,
                                 @QueryParam("encoding") @DefaultValue("utf8") String encoding,
                                 @QueryParam("start") @DefaultValue("0") int start,
                                 @QueryParam("count") @DefaultValue("50") int count,
                                 @QueryParam("sort") String sort) throws IOException {

        try {
            Fields fields = readerManager.getFields(segment);
            Terms terms = fields.terms(field);

            if (terms == null)
                throw new WebApplicationException("No such field " + field, Response.Status.NOT_FOUND);

            TermsEnum te = getTermsEnum(terms, filter);

            if (te.next() == null) {
                return new TermsData(terms, Collections.emptyList(), encoding);
            }

            StreamEx<TermData> stream = toStream(te, encoding);
            if (sort != null) {
                stream = stream.sorted(toComparator(sort));
            }
            if (startTerm != null) {
                stream = stream.dropWhile(t -> t.term.compareTo(startTerm) < 0);
            }
            if (start != 0) {
                stream = stream.skip(start);
            }
            List<TermData> collected = stream.limit(count).collect(Collectors.toList());
            return new TermsData(terms, collected, encoding);
        }
        catch (NumberFormatException e) {
            throw new WebApplicationException("Field " + field + " cannot be decoded as " + encoding, Response.Status.BAD_REQUEST);
        }
    }

    private Comparator<TermData> toComparator(String sort) {
        String[] parts = sort.split(" ");
        if (parts.length != 1 && parts.length != 2) {
            throw new WebApplicationException("Illegal sort [" + sort + "]");
        }
        Comparator<TermData> comparator = null;
        if ("docFreq".equals(parts[0])) {
            comparator = Comparator.comparing(TermData::getDocFreq);
        }
        if ("totalTermFreq".equals(parts[0])) {
            comparator = Comparator.comparing(TermData::getTotalTermFreq);
        }
        if (comparator == null) {
            throw new WebApplicationException("Illegal sort [" + sort + "]");
        }
        if (parts.length == 2 && "desc".equals(parts[1])) {
            comparator = comparator.reversed();
        }
        return comparator;
    }

    private StreamEx<TermData> toStream(TermsEnum te, String encoding) throws IOException {
        return StreamEx.iterate(toTermData(te, encoding), Objects::nonNull, o -> {
            try {
                if (te.next() != null) {
                    return toTermData(te, encoding);
                } else {
                    return null;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private TermData toTermData(TermsEnum te, String encoding) throws IOException {
        return new TermData(BytesRefUtils.encode(te.term(), encoding), te.docFreq(), te.totalTermFreq());
    }

    private TermsEnum getTermsEnum(Terms terms, String filter) throws IOException {
        if (filter == null)
            return terms.iterator();

        CompiledAutomaton automaton = new CompiledAutomaton(new RegExp(filter).toAutomaton());
        return automaton.getTermsEnum(terms);
    }

}
