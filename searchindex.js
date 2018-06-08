Search.setIndex({docnames:["doc/cloud","doc/demo","doc/errors","doc/handle","doc/ids","doc/machine","doc/machine_runner","doc/message","doc/messages","doc/nodes","doc/recorded","doc/settings","doc/simulator","doc/spawners","doc/system_controller","doc/transport","doc/virtual","index"],envversion:53,filenames:["doc/cloud.rst","doc/demo.rst","doc/errors.rst","doc/handle.rst","doc/ids.rst","doc/machine.rst","doc/machine_runner.rst","doc/message.rst","doc/messages.rst","doc/nodes.rst","doc/recorded.rst","doc/settings.rst","doc/simulator.rst","doc/spawners.rst","doc/system_controller.rst","doc/transport.rst","doc/virtual.rst","index.rst"],objects:{"dist_zero.errors":{DistZeroError:[2,1,1,""],InternalError:[2,1,1,""],NoTransportError:[2,1,1,""],SimulationError:[2,1,1,""]},"dist_zero.exporter":{Exporter:[9,2,1,""]},"dist_zero.exporter.Exporter":{PENDING_EXPIRATION_TIME_MS:[9,3,1,""],duplicate:[9,4,1,""],export_message:[9,4,1,""],least_unacknowledged_sequence_number:[9,3,1,""],retransmit_expired_pending_messages:[9,4,1,""]},"dist_zero.ids":{new_id:[4,5,1,""]},"dist_zero.importer":{Importer:[9,2,1,""]},"dist_zero.importer.Importer":{acknowledge:[9,4,1,""],least_unreceived_remote_sequence_number:[9,3,1,""]},"dist_zero.machine":{MachineController:[5,2,1,""],NodeManager:[5,2,1,""]},"dist_zero.machine.MachineController":{new_transport:[5,4,1,""],send:[5,4,1,""],spawn_node:[5,4,1,""],transfer_transport:[5,4,1,""]},"dist_zero.machine.NodeManager":{MAX_POSTPONE_TIME_MS:[5,3,1,""],MIN_POSTPONE_TIME_MS:[5,3,1,""],elapse_nodes:[5,4,1,""],handle_api_message:[5,4,1,""],handle_message:[5,4,1,""],new_transport:[5,4,1,""],send:[5,4,1,""],spawn_node:[5,4,1,""],transfer_transport:[5,4,1,""]},"dist_zero.machine_init":{run_new_machine_runner_from_args:[13,5,1,""]},"dist_zero.machine_runner":{MachineRunner:[6,2,1,""]},"dist_zero.machine_runner.MachineRunner":{configure_logging:[6,4,1,""],node_manager:[6,3,1,""],runloop:[6,4,1,""]},"dist_zero.messages":{activate:[8,5,1,""],activate_output:[8,5,1,""]},"dist_zero.node":{io:[9,0,0,"-"],node:[9,0,0,"-"],sum:[9,0,0,"-"]},"dist_zero.node.io":{InternalNode:[9,2,1,""],LeafNode:[9,2,1,""]},"dist_zero.node.io.InternalNode":{added_leaf:[9,4,1,""],create_kid_config:[9,4,1,""],elapse:[9,4,1,""],initialize:[9,4,1,""],receive:[9,4,1,""]},"dist_zero.node.io.LeafNode":{elapse:[9,4,1,""],initialize:[9,4,1,""],receive:[9,4,1,""]},"dist_zero.node.node":{Node:[9,2,1,""]},"dist_zero.node.node.Node":{elapse:[9,4,1,""],initialize:[9,4,1,""],new_handle:[9,4,1,""],receive:[9,4,1,""],send:[9,4,1,""],transfer_handle:[9,4,1,""]},"dist_zero.node.sum":{SumNode:[9,2,1,""],SumNodeSenderSplitMigrator:[9,2,1,""]},"dist_zero.node.sum.SumNode":{SEND_INTERVAL_MS:[9,3,1,""],TIME_BETWEEN_ACKNOWLEDGEMENTS_MS:[9,3,1,""],TIME_BETWEEN_RETRANSMISSION_CHECKS_MS:[9,3,1,""],deliver:[9,4,1,""],elapse:[9,4,1,""],initialize:[9,4,1,""],migration_finished:[9,4,1,""],receive:[9,4,1,""]},"dist_zero.node.sum.SumNodeSenderSplitMigrator":{STATE_DUPLICATING_INPUTS:[9,3,1,""],STATE_FINISHED:[9,3,1,""],STATE_INITIALIZING_NEW_NODES:[9,3,1,""],STATE_NEW:[9,3,1,""],STATE_SYNCING_NEW_NODES:[9,3,1,""],STATE_TRIMMING_INPUTS:[9,3,1,""],finished_duplicating:[9,4,1,""],middle_node_duplicated:[9,4,1,""],middle_node_live:[9,4,1,""],middle_node_started:[9,4,1,""],start:[9,4,1,""]},"dist_zero.recorded":{RecordedUser:[10,2,1,""]},"dist_zero.recorded.RecordedUser":{elapse_and_get_messages:[10,4,1,""]},"dist_zero.spawners":{ALL_MODES:[13,6,1,""],MODE_CLOUD:[13,6,1,""],MODE_SIMULATED:[13,6,1,""],MODE_VIRTUAL:[13,6,1,""],docker:[16,0,0,"-"],simulator:[12,0,0,"-"],spawner:[13,0,0,"-"]},"dist_zero.spawners.cloud":{aws:[0,0,0,"-"]},"dist_zero.spawners.cloud.aws":{Ec2Spawner:[0,2,1,""]},"dist_zero.spawners.cloud.aws.Ec2Spawner":{create_machine:[0,4,1,""],create_machines:[0,4,1,""],mode:[0,4,1,""],send_to_machine:[0,4,1,""]},"dist_zero.spawners.docker":{DockerSpawner:[16,2,1,""]},"dist_zero.spawners.docker.DockerSpawner":{all_spawned_containers:[16,4,1,""],clean_all:[16,4,1,""],create_machine:[16,4,1,""],create_machines:[16,4,1,""],get_running_containers:[16,4,1,""],get_stopped_containers:[16,4,1,""],mode:[16,4,1,""],send_to_machine:[16,4,1,""],started:[16,3,1,""]},"dist_zero.spawners.simulator":{SimulatedSpawner:[12,2,1,""]},"dist_zero.spawners.simulator.SimulatedSpawner":{create_machine:[12,4,1,""],create_machines:[12,4,1,""],get_machine_controller:[12,4,1,""],mode:[12,4,1,""],run_for:[12,4,1,""],send_to_machine:[12,4,1,""],start:[12,4,1,""]},"dist_zero.spawners.spawner":{Spawner:[13,2,1,""]},"dist_zero.spawners.spawner.Spawner":{create_machine:[13,4,1,""],create_machines:[13,4,1,""],mode:[13,4,1,""],send_to_machine:[13,4,1,""]},"dist_zero.system_controller":{SystemController:[14,2,1,""]},"dist_zero.system_controller.SystemController":{create_kid:[14,4,1,""],create_kid_config:[14,4,1,""],create_machine:[14,4,1,""],create_machines:[14,4,1,""],generate_new_handle:[14,4,1,""],get_output_state:[14,4,1,""],send_to_node:[14,4,1,""],spawn_node:[14,4,1,""],spawner:[14,3,1,""]},"test.demo":{Demo:[1,2,1,""]},"test.demo.Demo":{new_machine_controller:[1,4,1,""],new_machine_controllers:[1,4,1,""],now_ms:[1,4,1,""],run_for:[1,4,1,""],simulated:[1,3,1,""],start:[1,4,1,""],tear_down:[1,4,1,""]},dist_zero:{errors:[2,0,0,"-"],exporter:[9,0,0,"-"],ids:[4,0,0,"-"],importer:[9,0,0,"-"],machine:[5,0,0,"-"],machine_init:[13,0,0,"-"],machine_runner:[6,0,0,"-"],messages:[8,0,0,"-"],recorded:[10,0,0,"-"],settings:[11,0,0,"-"],spawners:[13,0,0,"-"],system_controller:[14,0,0,"-"]},test:{demo:[1,0,0,"-"]}},objnames:{"0":["py","module","Python module"],"1":["py","exception","Python exception"],"2":["py","class","Python class"],"3":["py","attribute","Python attribute"],"4":["py","method","Python method"],"5":["py","function","Python function"],"6":["py","data","Python data"]},objtypes:{"0":"py:module","1":"py:exception","2":"py:class","3":"py:attribute","4":"py:method","5":"py:function","6":"py:data"},terms:{"1f057767":0,"abstract":[9,13],"case":9,"class":[0,1,2,5,6,9,10,12,13,14,16],"export":9,"final":9,"function":[4,5,8,9],"import":9,"int":[1,5,9,10,12],"long":[0,9],"new":[0,1,3,4,5,9,12,13,14,16],"null":9,"return":[0,1,4,5,9,12,13,14,16],"true":[1,16],For:[1,2,6,9,13],Ids:17,That:5,The:[0,1,2,5,6,8,9,10,12,13,14,16],Then:12,There:[9,13],These:9,Using:17,_import:9,abl:9,about:[14,15],access:11,accord:9,acknowled:9,acknowledg:9,action:9,activ:[5,8],activate_output:8,actual:9,add:[0,1,13],added:9,added_leaf:9,addit:5,adjac:9,after:9,against:10,all:[0,1,2,4,5,9,11,12,13,15,16],all_mod:13,all_spawned_contain:16,allow:[5,15],along:15,alreadi:9,also:[5,9],ami:0,among:4,amount:[9,10],ani:[1,5,9,15,16],anoth:[3,9,15],api:[0,5,12,13,16],appropri:[0,3,9],arbitrari:5,argument:13,argv:13,arriv:[5,9],associ:[9,12,14,16],asynchron:5,attach:9,avail:13,aws:0,aws_region:0,back:[10,14],base:[0,2,9,13],base_ami:0,base_config:1,been:[8,9,14],befor:9,begin:[9,12],behavior:[9,12],being:12,below:13,between:[9,13],both:9,build:8,call:[2,3,9,12],can:[3,5,9,10,13,14],central:4,check:9,child:9,clean_al:16,closest:9,cloud:[1,5,13,17],code:[9,13],collect:16,come:9,commond:13,commun:[0,13,15],complex:9,comput:9,config:[1,9,14],configur:[0,5,6,12,13,14,16,17],configure_log:6,confirm:9,connect:[0,12,13,15,16],consid:9,contain:[5,6,14,16],content:17,control:[1,9,17],correspond:9,could:9,creat:[0,1,3,4,5,9,12,13,14],create_kid:14,create_kid_config:[9,14],create_machin:[0,12,13,14,16],creation:5,cryptograph:15,current:[0,1,9,12,13,14,16],daemon:16,decid:9,decis:9,defin:[0,13],deliv:[5,9],demo:17,depend:[1,7],describ:5,design:[9,12,13],desir:9,desktop:9,destin:9,determin:13,devic:9,dict:1,dictionari:1,differ:13,dist:2,dist_zero:[0,2,4,5,6,8,9,10,12,13,14,16],distinct:5,distribut:[0,1,10,12,13,14,16],distzero:[2,11,13,16],distzeroerror:2,docker:16,dockerspawn:16,downstream:9,drop:5,duplic:9,duplicating_input:9,dure:[9,12],each:[0,2,5,6,7,9,13,15,16],earlier:5,ec2:0,ec2spawn:0,edg:[8,9],either:[0,1,3,5,9,12,13,16],elaps:[5,6,9,10],elapse_and_get_messag:10,elapse_nod:[5,6],emploi:12,encapsul:13,encrypt:9,end:9,enough:9,enter:[6,13],entir:[9,12,14],entrypoint:13,environ:17,error:[5,17],event:[5,9],eventu:9,ever:[9,16],everi:5,exactli:9,exampl:9,exc_info:2,except:[2,12],exist:[3,14],existing_node_id:14,expect:9,expir:9,export_messag:9,expos:5,exsit:14,extra:1,factori:12,fill:14,finish:9,finished_dupl:9,first:5,for_nod:9,for_node_id:[5,9],form:2,format:2,forward:9,from:[0,3,5,6,9,12,13,16],full:1,fulli:9,func:[5,9],fundament:9,gener:[2,4,5,9,12,14,17],generate_new_handl:14,get:[14,16],get_machine_control:12,get_output_st:14,get_running_contain:16,get_stopped_contain:16,given:9,goe:9,group:0,grow:9,handl:[0,1,5,8,9,12,13,14,16,17],handle_api_messag:[5,6],handle_messag:[5,6],hardwar:[5,9,16],has:[5,7,8,9,14,16],hasn:9,have:[3,9,13],help:[9,10],hidden:5,hold:3,host:[0,5,13,16],how:[5,13],howev:13,identifi:3,ids:4,iff:[1,16],imag:0,immedi:9,implement:5,import_messag:9,includ:15,increment:9,index:17,indic:[0,5,9,12,13,16],inform:15,initi:9,initial_st:9,initializing_new_nod:9,input:[8,9,17],input_nod:9,insid:6,instanc:[0,1,4,5,9,10,12,13,14,16],instance_typ:0,intanc:5,intend:9,interact:[5,9,10,13],interfac:[5,9],intern:[2,9],internal_node_id:14,internalerror:2,internalnod:[9,14],involv:9,ip_host:5,iter:6,its:[3,8,9,13],job:9,json:[0,3,5,7,9,12,13,15,16],just:[9,16],keep:9,kei:[3,7],kid:[9,14],known:2,laptop:9,larg:9,leaf:9,leafnod:9,least:9,least_unacknowledged_sequence_numb:9,least_unreceived_remote_sequence_numb:9,leav:9,leftmost:9,less:9,life:12,like:[1,9],line:2,link:5,list:[0,1,2,9,12,13,14,16],listen:[0,9,12,13,16],live:9,local:5,log:[2,6],log_lin:2,logger:9,loop:6,machin:[0,1,9,12,14,16,17],machine_config:[0,5,6,12,13,14,16],machine_controller_handl:[9,14],machine_init:[0,13],machine_runn:6,machinecontrol:[0,1,3,4,5,6,9,12,13,14,16],machinerunn:[5,6,16],mai:[1,9],main:13,manag:[0,5,9,12,13,14,16],mark:9,match:[0,12,13,14,16],max_postpone_time_m:5,maximum:5,meant:9,messag:[0,3,5,6,9,10,12,13,14,15,16,17],method:[5,9,13],micro:0,middl:9,middle_node_dupl:9,middle_node_handl:9,middle_node_id:9,middle_node_l:9,middle_node_start:9,migrat:9,migration_finish:9,millisecond:[1,5,9,10,12],min_postpone_time_m:5,minimum:5,mode:[0,1,5,12,13,16],mode_cloud:13,mode_simul:13,mode_virtu:13,model:9,modul:[4,11,13,17],motiv:9,msg:10,much:[9,13],must:[0,5,9],name:[9,10,14],natur:9,necessari:9,need:[4,15],network:[5,12,15],never:9,new_handl:[3,9],new_id:4,new_machine_control:1,new_node_id:14,new_node_nam:14,new_transport:5,newli:[1,5,14],node:[3,4,5,7,8,10,12,14,15,17],node_config:[5,14],node_handl:5,node_id:[9,14],node_manag:6,nodemanag:[5,6],non:16,none:[0,1,6,9,10,12,13,14,16],nonempti:9,note:9,notransporterror:2,now_m:1,number:[1,5,9,10,12],object:[0,3,5,7,9,12,13,14,15,16],obtain:3,often:4,old:9,on_machin:[5,14],onc:[9,13,14],one:[0,1,3,9,12,13,15,16],onli:[1,5,9,11,12,13],open:0,oper:13,option:14,order:[0,9,10,12,13,14,15,16],ordinari:5,origin:9,other:[4,5,7,9,11],output:[8,9,14],output_nod:[9,14],output_node_id:14,over:9,overal:[0,12,13],page:17,pair:10,paramet:[0,1,2,4,5,8,9,10,12,13,14,16],parent:[9,12,13,14],parent_node_id:14,part:1,particular:[9,14],pass:[6,9],passag:5,pend:9,pending_expiration_time_m:9,pending_sender_id:9,period:3,perspect:9,physic:[9,16],plai:[10,14],playback:9,port:[0,12,13,16],possibl:13,postpon:5,practic:9,pre:9,prefix:4,prepend:4,prerequisit:9,primarili:9,probabl:5,process:[0,5,9,12,13,16],product:16,proper:9,provid:[5,9],provis:[0,13],put:9,python:[11,12],random:12,random_se:[1,12],read:[6,9,13],real:[1,6,12],receiv:[3,5,8,9,15],receiver_id:2,recipi:9,record:[9,14,17],recorded_us:[9,14],recordedus:[9,10,14],refer:4,referenc:9,region:0,regist:9,remaining_sender_id:9,remote_sequence_numb:9,remov:[1,9,16],reorder:5,replac:9,report:9,repres:[3,7,9,10,15],resid:16,resourc:[1,16],respect:9,respond:5,respons:[0,5,9,12,13,16],resubmit:9,result:2,retransmiss:9,retransmit:9,retransmit_expired_pending_messag:9,right:9,root:9,run:[0,1,5,6,9,12,13,14,16],run_for:[1,12],run_new_machine_runner_from_arg:13,runloop:[5,6,13],runner:[5,17],same:[0,5,9,12,13,14,16],search:17,second:5,secur:[0,15],security_group:0,see:9,seed:12,self:[5,9],send:[0,3,5,7,8,9,12,13,14,15,16],send_interval_m:9,send_to_machin:[0,5,12,13,16],send_to_nod:14,sender:[3,5,8,9],sender_id:[2,9],sending_nod:5,sending_node_id:5,sending_to:9,sent:9,separ:9,sequenc:9,serializ:[0,3,5,7,9,12,13,15,16],set:[0,8,9],share:13,should:[3,9,11,12,13,14,15,16],shrink:9,simul:[1,5,13,16,17],simulatedspawn:[2,5,12],simulationerror:[2,12],singl:[5,9,12,15],smaller:9,sock_typ:[0,12,13,16],socket:[5,6],some:[0,5,9,12,13,14,16],sourc:[0,1,2,4,5,6,8,9,10,12,13,14,16],spaw:9,spawn:[0,9,12,13,14,16],spawn_nod:[5,14],spawner:[14,17],spawning_migr:9,spin:[0,9,16],standard:8,start:[0,1,9,12,13,14,16],state:[9,14],state_duplicating_input:9,state_finish:9,state_initializing_new_nod:9,state_new:9,state_syncing_new_nod:9,state_trimming_input:9,still:9,stop:9,store:11,str:[0,1,4,5,9,12,13,14,16],string:2,structur:9,sub:13,subclass:[0,13,14],subtre:9,suitabl:5,sum:9,sum_nod:9,sumnod:9,sumnodesendersplitmigr:9,sync:9,syncing_new_nod:9,sys:[2,13],system:[0,1,5,9,10,12,13,16,17],system_control:14,system_id:[0,12,14,16],systemcontrol:14,take:9,tcp:[0,12,13,16],tear_down:1,technolog:16,terminolog:9,test:[1,9,10,12,14],than:9,thei:[9,13],them:[0,9,10,12,13,14,16],thi:[0,1,4,5,6,9,10,11,12,13,14,16],thing:4,three:[9,13],thrown:12,thu:9,time:[1,3,5,6,9,10,14],time_action_pair:10,time_between_acknowledgements_m:9,time_between_retransmission_checks_m:9,time_m:9,too:9,total:9,track:[9,13],tradit:9,transfer:3,transfer_handl:[3,9],transfer_transport:5,transport:[5,8,17],tree:9,tri:9,trigger:[5,9],trimming_input:9,tupl:2,two:[5,9],type:[0,1,3,4,5,7,9,12,13,14,16],typic:[3,12],udp:[0,12,13,16],underli:[5,6,9,12,14],uniniti:9,uniqu:[5,13,15,17],unit:9,updat:9,update_st:9,use:[0,3,5,9,11,12,14],used:[3,7,9,11,12,15],user:[9,14,17],valu:7,variabl:11,variant:9,variou:[5,7],via:[5,9],virtual:[1,5,13,17],wai:[5,9,13],wait:9,watch:9,whatev:5,when:[0,5,8,9],where:[5,9,10],wherea:9,whether:9,which:[0,1,9,12,13,14,16],wish:3,within:12,work:4,would:9,wrap:12,write:9,yet:14,yield:10,zero:2},titles:["Cloud Spawners","Demos","Errors","Handle","Unique Ids","Machine","Machine Runner","Message","Messages","Nodes","Using Recordings and User-input Generators to Simulate User Input","Environment Configuration","Simulated Spawner","Spawners and Machine Controllers","System Controller","Transport","Virtual Spawner","Welcome to DistZero\u2019s documentation!"],titleterms:{Ids:4,Using:10,cloud:0,configur:11,control:[13,14],demo:1,distzero:17,document:17,environ:11,error:2,gener:10,handl:3,indic:17,input:10,machin:[5,6,13],messag:[7,8],node:9,record:10,runner:6,simul:[10,12],spawner:[0,12,13,16],system:14,tabl:17,transport:15,uniqu:4,user:10,virtual:16,welcom:17}})