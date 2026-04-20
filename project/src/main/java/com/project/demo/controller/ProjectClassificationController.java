package com.project.demo.controller;

import com.project.demo.entity.ProjectClassification;
import com.project.demo.service.ProjectClassificationService;
import com.project.demo.controller.base.BaseController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import javax.persistence.Query;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.*;


/**
 * 项目分类：(ProjectClassification)表控制层
 *
 */
@RestController
@RequestMapping("/project_classification")
public class ProjectClassificationController extends BaseController<ProjectClassification, ProjectClassificationService> {

    /**
     * 项目分类对象
     */
    @Autowired
    public ProjectClassificationController(ProjectClassificationService service) {
        setService(service);
    }



    @PostMapping("/add")
    @Transactional
    public Map<String, Object> add(HttpServletRequest request) throws IOException {
        Map<String,Object> paramMap = service.readBody(request.getReader());
        this.addMap(paramMap);
        return success(1);
    }


}
