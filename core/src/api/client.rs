
#[derive(Debug)]
pub struct ConnectClient {
    configuration: Arc<ConnectClientConfiguration>,
    channel: Channel,
    user_context: UserContext,
    stub_state: ConnectStubState,
    bstub: CustomConnectBlockingStub,
    stub: CustomConnectStub,
    session_id: String,
    artifact_manager: ArtifactManager,
    tags: Arc<InheritableThreadLocal<HashSet<String>>>,
}

impl ConnectClient {
    pub fn new(configuration: Arc<ConnectClientConfiguration>, channel: Channel) -> Self {
        let user_context = configuration.user_context.clone();
        let stub_state = ConnectStubState::new(channel.clone(), configuration.retry_policies.clone());
        let bstub = CustomConnectBlockingStub::new(channel.clone(), stub_state.clone());
        let stub = CustomConnectStub::new(channel.clone(), stub_state.clone());
        let session_id = match configuration.session_id {
            Some(id) => id,
            None => Uuid::new_v4().to_string(),
        };
        let artifact_manager = ArtifactManager::new(configuration.clone(), session_id.clone(), bstub.clone(), stub.clone());
        let tags = Arc::new(InheritableThreadLocal::new(HashSet::new()));

        ConnectClient {
            configuration,
            channel,
            user_context,
            stub_state,
            bstub,
            stub,
            session_id,
            artifact_manager,
            tags,
        }
    }

    pub fn upload_all_class_file_artifacts(&self) {
        self.artifact_manager.upload_all_class_file_artifacts();
    }

    pub fn analyze(&self, request: AnalyzePlanRequest) -> AnalyzePlanResponse {
        self.artifact_manager.upload_all_class_file_artifacts();
        self.bstub.analyze_plan(request)
    }

    pub fn execute(&self, plan: Plan) -> CloseableIterator<ExecutePlanResponse> {
        self.artifact_manager.upload_all_class_file_artifacts();
        let request = ExecutePlanRequest {
            plan: Some(plan),
            user_context: Some(self.user_context.clone()),
            session_id: self.session_id.clone(),
            client_type: self.configuration.user_agent.clone(),
            tags: self.tags.get().iter().map(|tag| tag.clone()).collect(),
        };
        if self.configuration.use_reattachable_execute {
            self.bstub.execute_plan_reattachable(request)
        } else {
            self.bstub.execute_plan(request)
        }
    }

    pub fn config(&self, operation: ConfigRequestOperation) -> ConfigResponse {
        let request = ConfigRequest {
            operation: Some(operation),
            session_id: self.session_id.clone(),
            client_type: self.configuration.user_agent.clone(),
            user_context: Some(self.user_context.clone()),
        };
        self.bstub.config(request)
    }

    fn analyze(
        method: proto::AnalyzePlanRequestAnalyzeCase,
        plan: Option<proto::Plan>,
        explain_mode: Option<proto::AnalyzePlanRequestExplainMode>,
    ) -> proto::AnalyzePlanResponse {
        let mut builder = proto::AnalyzePlanRequest::new();
        match method {
            proto::AnalyzePlanRequestAnalyzeCase::SCHEMA => {
                assert!(plan.is_some());
                builder.set_schema(
                    proto::AnalyzePlanRequestSchema::new()
                        .set_plan(plan.unwrap())
                        .build(),
                );
            }
            proto::AnalyzePlanRequestAnalyzeCase::EXPLAIN => {
                if explain_mode.is_none() {
                    panic!("ExplainMode is required in Explain request");
                }
                assert!(plan.is_some());
                builder.set_explain(
                    proto::AnalyzePlanRequestExplain::new()
                        .set_plan(plan.unwrap())
                        .set_explain_mode(explain_mode.unwrap())
                        .build(),
                );
            }
            proto::AnalyzePlanRequest_AnalyzeCase::IS_LOCAL => {
                assert!(plan.is_some());
                builder.set_is_local(
                    proto::AnalyzePlanRequestIsLocal::new()
                        .set_plan(plan.unwrap())
                        .build(),
                );
            }
            proto::AnalyzePlanRequestAnalyzeCase::IS_STREAMING => {
                assert!(plan.is_some());
                builder.set_is_streaming(
                    proto::AnalyzePlanRequestIsStreaming::new()
                        .set_plan(plan.unwrap())
                        .build(),
                );
            }
            proto::AnalyzePlanRequestAnalyzeCase::INPUT_FILES => {
                assert!(plan.is_some());
                builder.set_input_files(
                    proto::AnalyzePlanRequestInputFiles::new()
                        .set_plan(plan.unwrap())
                        .build(),
                );
            }
            proto::AnalyzePlanRequestAnalyzeCase::ATOMIC_VERSION => {
                builder.set_version(proto::AnalyzePlanRequestVersion::new().build());
            }
            _ => panic!("Unknown Analyze request {:?}", method),
        }
        analyze(builder)
    }
    
    fn same_semantics(plan: proto::Plan, other_plan: proto::Plan) -> proto::AnalyzePlanResponse {
        let mut builder = proto::AnalyzePlanRequest::new();
        builder.set_same_semantics(
            proto::AnalyzePlanRequestSameSemantics::new()
                .set_target_plan(plan)
                .set_other_plan(other_plan),
        );
        analyze(builder)
    }
    
    fn semantic_hash(plan: proto::Plan) -> proto::AnalyzePlanResponse {
        let mut builder = proto::AnalyzePlanRequest::new();
        builder.set_semantic_hash(
            proto::AnalyzePlanRequestSemanticHash::new().set_plan(plan),
        );
        analyze(builder)
    }
    
    fn analyze(
        builder: proto::AnalyzePlanRequest_Builder,
    ) -> proto::AnalyzePlanResponse {
        let request = builder
            .set_user_context(user_context)
            .set_session_id(session_id)
            .set_client_type(user_agent)
            .build();
        analyze(request)
    }

}