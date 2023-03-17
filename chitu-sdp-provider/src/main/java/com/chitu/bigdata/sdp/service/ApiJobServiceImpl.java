package com.chitu.bigdata.sdp.service;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.chitu.bigdata.sdp.api.bo.*;
import com.chitu.bigdata.sdp.api.domain.*;
import com.chitu.bigdata.sdp.api.enums.*;
import com.chitu.bigdata.sdp.api.flink.Checkpoint;
import com.chitu.bigdata.sdp.api.model.*;
import com.chitu.bigdata.sdp.api.vo.SdpFileResp;
import com.chitu.bigdata.sdp.config.FlinkConfigProperties;
import com.chitu.bigdata.sdp.constant.BusinessFlag;
import com.chitu.bigdata.sdp.constant.FlinkConfigKeyConstant;
import com.chitu.bigdata.sdp.flink.common.util.DeflaterUtils;
import com.chitu.bigdata.sdp.mapper.*;
import com.chitu.bigdata.sdp.utils.Assert;
import com.chitu.bigdata.sdp.utils.JobOperationUtils;
import com.chitu.cloud.exception.ApplicationException;
import com.chitu.cloud.model.ResponseData;
import com.xiaoleilu.hutool.date.DateUtil;
import com.xiaoleilu.hutool.util.CollectionUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.assertj.core.util.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

import static com.chitu.bigdata.sdp.utils.DateUtils.YYYY_MM_DD_HH_MM_SS;

/**
 * @author chenyun
 * @description: TODO
 * @date 2022/2/24 11:34
 */
@Slf4j
@Service
public class ApiJobServiceImpl {
    @Autowired
    private FlinkConfigProperties configProperties;
    @Autowired
    private JobService jobService;
    @Autowired
    private FileService fileService;
    @Autowired
    private SdpFolderMapper folderMapper;
    @Autowired
    private SdpProjectMapper projectMapper;
    @Autowired
    private SdpEngineMapper engineMapper;
    @Autowired
    private SdpJobInstanceMapper instanceMapper;
    @Autowired
    JobInstanceService jobInstanceService;

    @Autowired
    private SdpJobMapper jobMapper;
    @Autowired
    private ProjectService projectService;
    @Autowired
    private SdpUserMapper userMapper;
    @Autowired
    EngineService engineService;
    @Autowired
    SdpRuntimeLogMapper runtimeLogMapper;
    @Autowired
    JobRunResultService runResultService;
    @Autowired
    MetaTableConfigService metaTableConfigService;


    @Transactional(rollbackFor = Exception.class)
    public ResponseData saveJob(ApiJobBO jobBO) {
        ResponseData data = new ResponseData<>();
        SdpFileBO fileBO = new SdpFileBO();
        //首先初始化当前业务线的用户
        SdpUser sdpuser = new SdpUser();
        sdpuser.setUserName(BusinessFlag.valueOf(jobBO.getBusinessFlag()).getFlag());
        SdpUser user = userMapper.selectIsExist(sdpuser);
        if (user != null) {
            jobBO.setUserId(user.getId() + "");
        }
        //查询引擎
        SdpEngine params = new SdpEngine();
        params.setBusinessFlag(jobBO.getBusinessFlag());
        SdpEngine sdpEngine = engineMapper.getInfo(params);
        SdpProject sdpProject = assembleProjectId(jobBO, fileBO, sdpEngine);
        assembleFolderId(jobBO, fileBO, sdpProject);
        JobConfig jobConfig = new JobConfig();
        String flinkYaml = jobBO.getFlinkYaml();
        if (StringUtils.isEmpty(flinkYaml)) {
            flinkYaml = DeflaterUtils.unzipString(configProperties.getSqlTemplate());
        }
        jobConfig.setFlinkYaml(flinkYaml);
        if (null != sdpEngine) {
            jobConfig.setEngineId(sdpEngine.getId());
            jobConfig.setEngine(sdpEngine.getEngineName());
            jobConfig.setVersion(sdpEngine.getEngineVersion());
        }
        if (StrUtil.isEmpty(jobBO.getFlinkVersion())) {
            jobConfig.setFlinkVersion(FlinkVersion.VERSION_114.getVersion());
        } else {
            jobConfig.setFlinkVersion(jobBO.getFlinkVersion());
        }
        fileBO.setEtlContent(jobBO.getFlinkSQL());
        fileBO.setFileType(FileType.SQL_STREAM.getType());
        fileBO.setBusinessFlag(jobBO.getBusinessFlag());
        fileBO.setFileName(jobBO.getJobName());
        fileBO.setJobConfig(jobConfig);
        fileBO.setSourceConfig(jobBO.getSourceConfig());
        fileBO.setUserId(Long.valueOf(jobBO.getUserId()));
        if (null != jobBO.getFileId()) {
            fileBO.setId(jobBO.getFileId());
            updateTask(data, fileBO);
        } else {
            if (null == jobBO.getSourceConfig()) {
                fileBO.setSourceConfig(new SourceConfig());
            }
            addTask(data, fileBO);
        }
        return data;
    }

    @Transactional(rollbackFor = Exception.class)
    public void addTask(ResponseData data, SdpFileBO fileBO) {
        SdpFile addFile = fileService.addFile(fileBO);
        if (addFile != null) {
            fileBO.setId(addFile.getId());
            List validate = fileService.explainSql(fileBO);
            if (validate.size() == 0) {
                SdpJob sdpJob = fileService.online(fileBO);
                if (null != sdpJob) {
                    addFile.setJobId(sdpJob.getId());
                    data.setData(addFile);
                    data.ok();
                } else {
                    data.setMsg("保存任务失败");
                    //校验失败时，删除已经入库的file
                    fileService.deleteFile(fileBO);
                }
            } else {
                data.setData(validate);
                data.setMsg("任务校验失败");
                data.setCode(ResponseCode.SQL_NOT_PASS.getCode());
                //校验失败时，删除已经入库的file
                fileService.deleteFile(fileBO);
            }
        } else {
            data.setMsg("保存任务失败");
        }
    }

    @Transactional(rollbackFor = Exception.class)
    public void updateTask(ResponseData data, SdpFileBO fileBO) {
        SdpFileResp updateFile = fileService.updateFile(fileBO);
        if (updateFile != null) {
            List validate = fileService.explainSql(fileBO);
            if (validate.size() == 0) {
                SdpJob sdpJob = fileService.online(fileBO);
                if (sdpJob != null) {
                    fileBO.setJobId(sdpJob.getId());
                    data.setData(fileBO);
                    data.ok();
                } else {
                    data.setMsg("修改任务失败");
                }
            } else {
                data.setData(validate);
                data.setMsg("任务校验失败");
                data.setCode(ResponseCode.SQL_NOT_PASS.getCode());
            }
        } else {
            data.setMsg("修改任务失败");
        }
    }

    @Transactional(rollbackFor = Exception.class)
    public SdpProject assembleProjectId(ApiJobBO jobBO, SdpFileBO fileBO, SdpEngine sdpEngine) {
        String businessFlag = jobBO.getBusinessFlag();
        SdpProject sdpProject = projectMapper.getProjectByCode(jobBO.getProjectCode());
        if (null == sdpProject) {
            SdpProjectInfo sdpProject1 = new SdpProjectInfo();
            sdpProject1.setProjectCode(jobBO.getProjectCode());
            sdpProject1.setProjectName(BusinessFlag.valueOf(businessFlag).getFlag() + "_" + jobBO.getProjectCode());
            sdpProject1.setBusinessFlag(businessFlag);
            sdpProject1.setEnabledFlag(1L);
            sdpProject1.setCreatedBy(jobBO.getUserId());
            sdpProject1.setUpdatedBy(jobBO.getUserId());
            sdpProject1.setCreationDate(new Timestamp(System.currentTimeMillis()));
            sdpProject1.setUpdationDate(new Timestamp(System.currentTimeMillis()));
            sdpProject1.setProjectEngines(Lists.newArrayList(sdpEngine));
            if (jobBO.getUserId() != null) {
                SdpUser user = userMapper.selectById(Long.valueOf(jobBO.getUserId()));
                if (user == null) {
                    List<SdpUser> leaders = userMapper.getCondition(null);
                    sdpProject1.setProjectLeader(leaders);
                    user = leaders.get(0);
                }
                ProjectUser user1 = new ProjectUser();
                user1.setUserName(user.getUserName());
                user1.setEmployeeNumber(user.getEmployeeNumber());
                sdpProject1.setProjectUsers(Lists.newArrayList(user1));
            } else {
                List<SdpUser> leaders = userMapper.getCondition(null);
                sdpProject1.setProjectLeader(leaders);
                SdpUser user = leaders.get(0);
                ProjectUser user1 = new ProjectUser();
                user1.setUserName(user.getUserName());
                user1.setEmployeeNumber(user.getEmployeeNumber());
                sdpProject1.setProjectUsers(Lists.newArrayList(user1));
            }
            sdpProject = projectService.add(sdpProject1);
        }
        fileBO.setProjectId(sdpProject.getId());
        return sdpProject;
    }

    @Transactional(rollbackFor = Exception.class)
    public void assembleFolderId(ApiJobBO jobBO, SdpFileBO fileBO, SdpProject sdpProject) {
        String businessFlag = jobBO.getBusinessFlag();
        SdpFolder params = new SdpFolder();
        params.setFolderName(jobBO.getFolderName());
        params.setProjectId(sdpProject.getId());
        params.setBusinessFlag(businessFlag);
        SdpFolder sdpFolder = folderMapper.getFolder(params);
        if (null == sdpFolder) {
            sdpFolder = new SdpFolder();
            sdpFolder.setFolderName(jobBO.getFolderName());
            sdpFolder.setProjectId(sdpProject.getId());
            sdpFolder.setParentId(0L);
            sdpFolder.setEnabledFlag(1L);
            sdpFolder.setBusinessFlag(businessFlag);
            sdpFolder.setCreatedBy(jobBO.getUserId());
            sdpFolder.setUpdatedBy(jobBO.getUserId());
            sdpFolder.setCreationDate(new Timestamp(System.currentTimeMillis()));
            sdpFolder.setUpdationDate(new Timestamp(System.currentTimeMillis()));
            folderMapper.insert(sdpFolder);
        }
        fileBO.setFolderId(sdpFolder.getId());
    }

    @Transactional(rollbackFor = Exception.class)
    public ResponseData startJob(ApiJobBO apiJob) {
        log.info("启动数据集成任务的入参:{}", JSON.toJSONString(apiJob));
        ResponseData data = new ResponseData<>();
        data.ok();
        try {
            handleParam(apiJob);
        } catch (Exception e) {
            log.error("数据集成指定启动参数处理异常", e);
            throw new ApplicationException(ResponseCode.COMMON_ERR, "指定启动参数启动异常");
        }
        String startMode = apiJob.getStartMode();
        Long jobId = apiJob.getId();
        SdpJobBO jobBO = new SdpJobBO();
        SdpJob sdpJob = new SdpJob();
        sdpJob.setId(jobId);
        jobBO.setVo(sdpJob);
        jobBO.setUseLatest(true);
        try {
//            if (engineService.checkResource(jobId).getSuccess()) {
            SdpJobInstance instance = instanceMapper.queryByJobId(jobId);
            if (null != instance && instance.getFlinkJobId() != null) {
                //如果存在检查点，则为恢复，否则为启动
                //String checkPointPrefix = configProperties.getDefaultMap().get(FlinkConfigKeyConstant.CHECKPOINT_DIR) + instance.getFlinkJobId();
                //if (HdfsUtils.exists(checkPointPrefix)) {
                //String childPath = HdfsUtils.getCheckPointPath(checkPointPrefix);
                List<Checkpoint> checkpoints = jobService.getCheckpoints(jobId, 1);
                if (!CollectionUtils.isEmpty(checkpoints)) {
                    //当检查点不为空，并且没有指定消费offset，才使用恢复，否则一律从头开始，即启动
                    if (StringUtils.isNotBlank(checkpoints.get(0).getFilePath()) && (!startMode.equals(FlinkConfigKeyConstant.TIMESTAMP) && !startMode.equals(FlinkConfigKeyConstant.SPECIFIC_OFFSETS) && !startMode.equals(FlinkConfigKeyConstant.EARLIEST_OFFSET))) {
                        JobOperationUtils.runInJobLock(jobId, JobOperationUtils.OperationType.RECOVER_JOB, () -> {
                            jobService.validateStart(jobBO, JobAction.RECOVER.toString());
                            jobService.startJob(jobBO, JobAction.RECOVER.toString());
                            return null;
                        });
                    } else {
                        JobOperationUtils.runInJobLock(jobId, JobOperationUtils.OperationType.START_JOB, () -> {
                            jobService.validateStart(jobBO, JobAction.START.toString());
                            jobService.startJob(jobBO, JobAction.START.toString());
                            return null;
                        });
                    }
                } else {
                    JobOperationUtils.runInJobLock(jobId, JobOperationUtils.OperationType.START_JOB, () -> {
                        jobService.validateStart(jobBO, JobAction.START.toString());
                        jobService.startJob(jobBO, JobAction.START.toString());
                        return null;
                    });
                }
            } else {
                JobOperationUtils.runInJobLock(jobId, JobOperationUtils.OperationType.START_JOB, () -> {
                    jobService.validateStart(jobBO, JobAction.START.toString());
                    jobService.startJob(jobBO, JobAction.START.toString());
                    return null;
                });
            }
//            }
        } catch (Exception e) {
            log.error("启动作业失败===jobId: " + jobId, e);
            if (e instanceof ApplicationException && ((ApplicationException) e).getAppCode().getCode() == ResponseCode.CONNECT_IS_WRONG.getCode()) {
                throw ((ApplicationException) e);
            } else {
                throw new ApplicationException(ResponseCode.CONNECT_IS_WRONG, String.format("启动作业失败, jobId: %d, message: %s", jobId, e.getMessage()));
            }
        }
        return data;
    }

    @Transactional(rollbackFor = Exception.class)
    public void handleParam(ApiJobBO apiJob) throws Exception {
        SdpJob sdpJob1 = jobMapper.selectById(apiJob.getId());
        if (sdpJob1 == null) {
            throw new ApplicationException(ResponseCode.JOB_NOT_EXISTS);
        }
        String flinkSql = sdpJob1.getJobContent();
        /*String regex;
        String flinkSql = sdpJob1.getJobContent();
        StringBuilder builder = new StringBuilder(flinkSql);
        int index = builder.indexOf("scan");
        String startMode = apiJob.getStartMode();
        if(startMode.equals(FlinkConfigKeyConstant.TIMESTAMP)){
            String startTimestamp = apiJob.getStartTimestamp();
            if(StringUtils.isEmpty(startTimestamp)){
                throw new ApplicationException(ResponseCode.JOB_PARAMS_MISSING,FlinkConfigKeyConstant.STARTUP_TIMESTAMP);
            }
            if(flinkSql.contains(FlinkConfigKeyConstant.STARTUP_TIMESTAMP)){
                regex = "\\d{13}";
                flinkSql = flinkSql.replaceAll(regex,startTimestamp);
                builder = new StringBuilder(flinkSql);
            }else{
                String extraOption = "  '"+FlinkConfigKeyConstant.STARTUP_TIMESTAMP+"' = '"+startTimestamp+"',\n";
                builder.insert(index-3,extraOption);
            }
        }else if(startMode.equals(FlinkConfigKeyConstant.SPECIFIC_OFFSETS)){
            String startOffsets = apiJob.getStartOffsets();
            if(StringUtils.isEmpty(startOffsets)){
                throw new ApplicationException(ResponseCode.JOB_PARAMS_MISSING,FlinkConfigKeyConstant.STARTUP_OFFSETS);
            }
            if(flinkSql.contains(FlinkConfigKeyConstant.STARTUP_OFFSETS)){
                regex = "((partition:)\\d{1,}(,offset:)\\d{1,};{0,}){1,}";
                flinkSql = flinkSql.replaceAll(regex,startOffsets);
                builder = new StringBuilder(flinkSql);
            }else{
                String extraOption = "  '"+FlinkConfigKeyConstant.STARTUP_OFFSETS+"' = '"+startOffsets+"',\n";
                builder.insert(index-3,extraOption);
            }
        }
        regex = "'([a-z]){5,8}\\-(offset)s{0,}'";
        flinkSql = builder.toString().replaceAll("'timestamp'","'"+startMode+"'").replaceAll(regex,"'"+startMode+"'");
        if(!startMode.equals(FlinkConfigKeyConstant.SPECIFIC_OFFSETS)){
            if(flinkSql.contains(FlinkConfigKeyConstant.STARTUP_OFFSETS)){
                regex = "'scan.startup.specific-offsets' = '((partition:)\\d{1,}(,offset:)\\d{1,};{0,}){1,}',\r\n";
                flinkSql = flinkSql.replaceAll(regex,"");
            }
        }
        if(!startMode.equals(FlinkConfigKeyConstant.TIMESTAMP)){
            if(flinkSql.contains(FlinkConfigKeyConstant.STARTUP_TIMESTAMP)){
                regex = "'scan.startup.timestamp-millis' = '\\d{13}',\r\n";
                flinkSql = flinkSql.replaceAll(regex,"");
            }
        }*/
        List<SourceKafkaInfo> createTableList4SourceKafkaInfo = jobService.getCreateTableList4SourceKafkaInfo(flinkSql);
        for (SourceKafkaInfo sourceKafkaInfo : createTableList4SourceKafkaInfo) {
            sourceKafkaInfo.setStartupMode(apiJob.getStartMode());
            String startTimestamp = apiJob.getStartTimestamp();
            if (StringUtils.isNotBlank(startTimestamp)) {
                Date date = new Date(Long.valueOf(startTimestamp));
                sourceKafkaInfo.setDateStr(DateUtil.format(date, YYYY_MM_DD_HH_MM_SS));
            }
            sourceKafkaInfo.setOffsets(apiJob.getStartOffsets());
        }
        Map<String, SourceKafkaInfo> tableMap = createTableList4SourceKafkaInfo.stream().collect(Collectors.toMap(SourceKafkaInfo::getFlinkTableName, m -> m, (k1, k2) -> k2));

        JobService.FlinkSqlBuilder flinkSqlBuilder = jobService.modifyOption(tableMap, flinkSql);

        sdpJob1.setJobContent(flinkSqlBuilder.toSqlString());
        jobService.updateSelective(sdpJob1);
    }

    public ResponseData stopJob(ApiJobBO apiJob) {
        log.info("停止数据集成任务的入参:{}", JSON.toJSONString(apiJob));
        ResponseData data = new ResponseData<>();
        data.ok();
        SdpJobBO jobBO = new SdpJobBO();
        SdpJob sdpJob = new SdpJob();
        sdpJob.setId(apiJob.getId());
        jobBO.setVo(sdpJob);
        try {
            JobOperationUtils.runInJobLock(apiJob.getId(), JobOperationUtils.OperationType.PAUSE_JOB, () -> {
                jobService.validateStop(jobBO, JobAction.PAUSE.toString());
                jobService.stopJob(jobBO, JobAction.PAUSE.toString());
                return null;
            });
        } catch (Exception e) {
            log.error("停止作业失败===jobId: " + apiJob.getId(), e);
            throw new ApplicationException(ResponseCode.ERROR, String.format("停止作业失败, jobId: %d, message: %s", apiJob.getId(), e.getMessage()));
        }
        return data;
    }

    public ResponseData offlineJob(ApiJobBO apiJob) {
        log.info("下线数据集成任务的入参:{}", JSON.toJSONString(apiJob));
        ResponseData data = new ResponseData<>();
        data.ok();
        SdpJobBO jobBO = new SdpJobBO();
        SdpJob sdpJob = new SdpJob();
        sdpJob.setId(apiJob.getId());
        jobBO.setVo(sdpJob);
        try {
            JobOperationUtils.runInJobLock(apiJob.getId(), JobOperationUtils.OperationType.STOP_JOB, () -> {
                jobService.validateStop(jobBO, JobAction.STOP.toString());
                jobService.stopJob(jobBO, JobAction.STOP.toString());
                return null;
            });
        } catch (Exception e) {
            log.error("下线作业失败===jobId: " + apiJob.getId(), e);
            throw new ApplicationException(ResponseCode.ERROR, String.format("下线作业失败, jobId: %d, message: %s", apiJob.getId(), e.getMessage()));
        }
        return data;
    }

    public ResponseData jobStatus(ApiJobBO jobBO) {
        ResponseData data = new ResponseData<>();
        List<SdpJobInstance> list = instanceMapper.queryByJobIds(jobBO.getIds());
        if (!CollectionUtils.isEmpty(list)) {
            list.stream().map(x -> {
                ApiJobStatus rawStatus = ApiJobStatus.fromStatus(x.getRawStatus());
                if (rawStatus.isReady()) {
                    x.setJobStatus(ApiJobStatus.READY.toString());
                } else if (rawStatus.isRunning()) {
                    x.setJobStatus(ApiJobStatus.RUNNING.toString());
                } else if (rawStatus.isTerminated()) {
                    x.setJobStatus(ApiJobStatus.TERMINATED.toString());
                } else {
                    x.setJobStatus(ApiJobStatus.FAILED.toString());
                }
                return x;
            }).collect(Collectors.toList());
            data.setData(list).ok();
        }
        return data;
    }

    public ResponseData jobRuntimeLogs(ApiJobBO apiJob) {
        ResponseData data = new ResponseData<>();
        data.ok();
        if (Objects.isNull(apiJob.getId())) {
            throw new ApplicationException(ResponseCode.LOG_LACK_ARGUMENT);
        }
        SdpRuntimeLogBO sdpRuntimeLogBO = new SdpRuntimeLogBO();
        sdpRuntimeLogBO.setJobId(apiJob.getId());
        ArrayList<SdpRuntimeLog> sdpRuntimeLogs = runtimeLogMapper.getDiLog(sdpRuntimeLogBO);
        if (CollectionUtils.isEmpty(sdpRuntimeLogs)) {
            data.setCode(9003);
            data.setData("运行日志无更新");
            return data;
        }
        SdpRuntimeLog runtimeLog = new SdpRuntimeLog();
        SdpRuntimeLog sdpRuntimeLog = sdpRuntimeLogs.get(0);
        runtimeLog.setJobId(sdpRuntimeLog.getJobId());
        //填充yarn的url
        for (SdpRuntimeLog log : sdpRuntimeLogs) {
            if (log.getOperation().equalsIgnoreCase(sdpRuntimeLog.getOperation())) {
                if (Objects.nonNull(log.getYarnLogUrl())) {
                    runtimeLog.setYarnLogUrl(log.getYarnLogUrl());
                    break;
                }
            } else {
                break;
            }

        }
        //根据实时状态去确认返回成功和失败
        if (JobAction.RECOVER.name().equalsIgnoreCase(sdpRuntimeLog.getOperation())) {
            runtimeLog.setOperation(JobAction.START.name());
            if (checkStatus(runtimeLog, sdpRuntimeLog)) {
                data.setCode(9003);
                data.setData("运行日志无更新");
                return data;
            }
        }
        if (JobAction.PAUSE.name().equalsIgnoreCase(sdpRuntimeLog.getOperation())) {
            runtimeLog.setOperation(JobAction.STOP.name());
        }
        if (JobAction.STOP.name().equalsIgnoreCase(sdpRuntimeLog.getOperation())) {
            runtimeLog.setOperation("OFFLINE");
        }
        if (JobAction.START.name().equalsIgnoreCase(sdpRuntimeLog.getOperation())) {
            runtimeLog.setOperation(sdpRuntimeLog.getOperation());
            if (checkStatus(runtimeLog, sdpRuntimeLog)) {
                data.setCode(9003);
                data.setData("运行日志无更新");
                return data;
            }
        }
        if (JobAction.ONLINE.name().equalsIgnoreCase(sdpRuntimeLog.getOperation()) ||
                JobAction.ADD_SAVEPOINT.name().equalsIgnoreCase(sdpRuntimeLog.getOperation())) {
            data.setCode(9003);
            data.setData("运行日志无更新");
            return data;
        }
        if (Objects.nonNull(sdpRuntimeLog.getStackTrace())) {
            runtimeLog.setStackTrace(sdpRuntimeLog.getStackTrace());
        }
        if (Objects.nonNull(sdpRuntimeLog.getOperationStage())) {
            runtimeLog.setOperationStage(sdpRuntimeLog.getOperationStage());
        }
        if (Objects.nonNull(sdpRuntimeLog.getFlinkLogUrl())) {
            runtimeLog.setFlinkLogUrl(sdpRuntimeLog.getFlinkLogUrl());
        }

        runtimeLog.setUpdationDate(sdpRuntimeLog.getCreationDate());
//        log.info("返回jobId:{}的日志信息为:{}",apiJob.getId(),JSON.toJSONString(runtimeLog));
        return data.setData(runtimeLog);
    }

    private boolean checkStatus(SdpRuntimeLog runtimeLog, SdpRuntimeLog sdpRuntimeLog) {
        if (LogStatus.SUCCESS.name().equalsIgnoreCase(sdpRuntimeLog.getOperationStage())) {
            SdpJobInstance sdpJobInstance = new SdpJobInstance();
            sdpJobInstance.setIsLatest(true);
            sdpJobInstance.setJobId(sdpRuntimeLog.getJobId());
            sdpJobInstance.setEnabledFlag(1L);
            List<SdpJobInstance> sdpJobInstances = jobInstanceService.selectAll(sdpJobInstance);
//            log.info("返回的实例:{}",JSON.toJSONString(sdpJobInstances.get(0)));
            if (!CollectionUtils.isEmpty(sdpJobInstances) && Objects.nonNull(sdpJobInstances.get(0).getJobStatus())) {
                String jobStatus = sdpJobInstances.get(0).getJobStatus();
                //没有instanceInfo的实例，就是还没有正常启动起来，不返回日志
                if (StrUtil.isBlank(sdpJobInstances.get(0).getInstanceInfo())) {
                    return true;
                }
                //如果instanceInfo的实例存在，并且不是running状态也不是INITIALIZE状态的就是失败
                if (JobStatus.INITIALIZE.name().equalsIgnoreCase(jobStatus)) {
                    return true;
                }
                if (!jobStatus.equalsIgnoreCase(JobStatus.RUNNING.name())) {
                    sdpRuntimeLog.setOperationStage(LogStatus.FAILED.name());
                }
            }
        }
        return false;
    }

    public ResponseData jobMonitorData(ApiJobBO apiJob) {
        if (Objects.isNull(apiJob.getId())) {
            throw new ApplicationException(ResponseCode.LOG_LACK_ARGUMENT);
        }
        return runResultService.queryJobRunningResult(apiJob.getId());
    }

    public ResponseData queryFailData(ApiJobBO apiJob) {
        if (Objects.isNull(apiJob.getId())) {
            throw new ApplicationException(ResponseCode.LOG_LACK_ARGUMENT);
        }
        JobRunResultFailDataBO jobRunResultFailDataBO = new JobRunResultFailDataBO();
        jobRunResultFailDataBO.setJobId(apiJob.getId());
        jobRunResultFailDataBO.setPage(apiJob.getPage());
        jobRunResultFailDataBO.setPageSize(apiJob.getPageSize());
        jobRunResultFailDataBO.setSourceType(apiJob.getSourceType());
        jobRunResultFailDataBO.setOrderByClauses(apiJob.getOrderByClauses());
        jobRunResultFailDataBO.setTableName(apiJob.getTableName());
        return runResultService.queryFailData(jobRunResultFailDataBO);
    }

    public ResponseData alertMonitorData(ApiJobBO apiJob) {
        if (Objects.isNull(apiJob.getIds()) || CollectionUtils.isEmpty(apiJob.getIds())) {
            throw new ApplicationException(ResponseCode.ERROR.getCode(), "入参的ids不能为空");
        }
        return runResultService.queryDiAlertJobsResult(apiJob.getIds());
    }


    @Transactional(rollbackFor = Exception.class)
    public ResponseData delJobFile(ApiJobBO apiJob) {

        Assert.notNull(apiJob.getId(), ResponseCode.INVALID_ARGUMENT, "id");
        SdpJob sdpJob = jobService.get(apiJob.getId());
        Assert.isTrue(BusinessFlag.DI.name().equals(sdpJob.getBusinessFlag()), ResponseCode.INVALID_ARGUMENT, "无法删除非数据集成的任务");

        // 校验任务实例是否在运行中
        SdpJobInstance originInstance = instanceMapper.queryByJobId(sdpJob.getId());
        if (originInstance != null) {
            Assert.isTrue(!JobStatus.RUNNING.toString().equals(originInstance.getJobStatus()), ResponseCode.JOB_IS_RUNNING);
        }

        // 先删除job
        jobService.deleteJob(Arrays.asList(sdpJob));

        if (sdpJob.getFileId() != null) {
            //删除文件和元表数据
            List<SdpMetaTableConfig> metaTableConfigs = metaTableConfigService.selectAll(new SdpMetaTableConfig(sdpJob.getFileId()));
            if (CollectionUtil.isNotEmpty(metaTableConfigs)) {
                Long[] ids = metaTableConfigs.stream().map(x -> x.getId()).toArray(Long[]::new);
                metaTableConfigService.disable(SdpMetaTableConfig.class,ids);
            }
            fileService.disable(SdpFile.class,sdpJob.getFileId());
        }


        ResponseData responseData = new ResponseData<>();
        responseData.ok();
        return responseData;
    }
}
