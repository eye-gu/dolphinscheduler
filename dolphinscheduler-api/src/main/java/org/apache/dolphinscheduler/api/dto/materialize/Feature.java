/*
 * Aloudata.com Inc.
 * Copyright (c) 2021-2021 All Rights Reserved.
 */

package org.apache.dolphinscheduler.api.dto.materialize;

import org.apache.dolphinscheduler.common.task.self.Param;

import java.util.List;

import lombok.Data;

/**
 * @author eye.gu@aloudata.com
 * @version 1
 * @date 2021-12-14 15:54
 */
@Data
public class Feature {

    private List<Param> globalParams;

}