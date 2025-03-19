package cn.edu.sustech.crawler;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class StanfordCoreNLPService {
    private final StanfordCoreNLP pipeline;

    public StanfordCoreNLPService() {
        Properties props = new Properties();
//        props.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner");
        props.setProperty("ner.useSUTime", "false");
        props.setProperty("annotators", "tokenize,ssplit"); // 去掉不必要的 POS 和 NER
        pipeline = new StanfordCoreNLP(props);
    }

    public Map<String, Integer> getAllJavaAPI(String htmlText) {
        ConcurrentMap<String, Integer> javaAPIs = new ConcurrentHashMap<>();
        Annotation annotation = new Annotation(htmlText);
        pipeline.annotate(annotation);
        List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);

        sentences.parallelStream().forEach(sentence -> {
            List<CoreLabel> tokens = sentence.get(CoreAnnotations.TokensAnnotation.class);
            for (CoreLabel token : tokens) {
                String word = token.get(CoreAnnotations.TextAnnotation.class);
                if (word.startsWith("java.")) {
                    // 使用同步块来保证线程安全
                    javaAPIs.put(word, javaAPIs.getOrDefault(word, 0) + 1);
                }
            }
        });

        return javaAPIs;
    }
}
