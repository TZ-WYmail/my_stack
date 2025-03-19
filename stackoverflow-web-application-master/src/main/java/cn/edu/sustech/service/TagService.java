package cn.edu.sustech.service;

import cn.edu.sustech.entity.Tag;
import cn.edu.sustech.entity.TagConnect;
import cn.edu.sustech.mapper.TagConnectMapper;
import cn.edu.sustech.mapper.TagMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class TagService {

  @Autowired private TagMapper tagMapper;

  @Autowired private TagConnectMapper connectMapper;

  public List<Tag> allTags() {
    return tagMapper.selectList(new QueryWrapper<>());
  }

  public List<Map<String, Object>> tagCounts() {
    QueryWrapper<TagConnect> wrapper = new QueryWrapper<>();
    wrapper.select("tag_name", "count(*) as count").groupBy("tag_name");
    return connectMapper.selectMaps(wrapper);
  }

  public List<Map<String, Object>> tagGroups() {
    QueryWrapper<TagConnect> wrapper = new QueryWrapper<>();
    wrapper
        .select("question_id", "string_agg(tag_name, ',' ORDER BY tag_name) as tag_group")
        .groupBy("question_id");
    return connectMapper.selectMaps(wrapper);
  }

  public List<TagConnect> tagsByQuestionId(int questionID) {
    QueryWrapper<TagConnect> wrapper = new QueryWrapper<>();
    wrapper.eq("question_id", questionID);
    return connectMapper.selectList(wrapper);
  }
}
