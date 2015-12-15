'use strict';

module.exports = function setUpRoutes(app) {

	if (typeof app !== 'object') {
		throw 'Missing app!';
	}
	if (typeof app.config !== 'object') {
		throw 'Missing app.config!';
	}
	if (typeof app.config['face-engine'] !== 'object') {
		throw 'Missing app.config[face-engine]!';
	}
	// Check face-engine config elements
	if (typeof app.config['face-engine']['bin-path'] !== 'string') {
		throw 'Missing app.config[face-engine][bin-path]!';
	}
	if (typeof app.config['face-engine']['output-path'] !== 'string') {
		throw 'Missing app.config[face-engine][output-path]!';
	}
	if (typeof app.config['face-engine']['appearance-metadata-file-name'] !== 'string') {
		throw 'Missing app.config[face-engine][appearance-metadata-file-name]!';
	}
	if (typeof app.config['face-engine'].fps !== 'number') {
		throw 'Missing app.config[face-engine].fps!';
	}
	if (typeof app.config['face-engine']['classifier-path'] !== 'string') {
		throw 'Missing app.config[face-engine][classifier-path]!';
	}
	if (typeof app.config['face-engine']['flandmark-model-path'] !== 'string') {
		throw 'Missing app.config[face-engine][flandmark-model-path]!';
	}
	if (typeof app.config['face-engine']['original-image-format'] !== 'string') {
		throw 'Missing app.config[face-engine][original-image-format]!';
	}
	if (typeof app.config['face-engine']['trained-image-format'] !== 'string') {
		throw 'Missing app.config[face-engine][trained-image-format]!';
	}
	if (typeof app.config['face-engine']['profile-image-format'] !== 'string') {
		throw 'Missing app.config[face-engine][profile-image-format]!';
	}
	if (typeof app.config['face-engine']['max-concurrent'] !== 'number') {
		throw 'Missing app.config[face-engine].[max-concurrent]!';
	}

	// Check S3 config elements
	if (typeof app.config.s3 !== 'object') {
		throw 'Missing app.config.s3!';
	}
	if (typeof app.config.s3.accessKey !== 'string') {
		throw 'Missing app.config.s3.accessKey!';
	}
	if (typeof app.config.s3.secretKey !== 'string') {
		throw 'Missing app.config.s3.secretKey!';
	}
	if (typeof app.config.s3.region !== 'string') {
		throw 'Missing app.config.s3.region!';
	}
	if (typeof app.config.s3.bucket !== 'string') {
		throw 'Missing app.config.s3.bucket!';
	}
	if (typeof app.logger !== 'object') {
		throw 'Missing app.logger!';
	}

	var s3 = require('./storage.s3')(app.config),
		Queue = require('./queue'),
		VeritoneApi = require('veritone-api'),
		FaceRecognitionClient = require('./faceRecognitionClient'),
		exec = require('child_process').exec,
		fs = require('fs'),
		uuid = require('node-uuid'),
		path = require('path'),
		async = require('async'),
		_ = require('underscore')._,
		rimraf = require('rimraf'),
		ffmpeg = require('ffmpeg');	

	var faceEngine = app.config['face-engine'],
		faceEnginePath = faceEngine['bin-path'],
		outputPath = faceEngine['output-path'],
		appearanceMetaDataFileName = faceEngine['appearance-metadata-file-name'],
		fps = faceEngine.fps,
		classifierPath = faceEngine['classifier-path'],
		flandmarkModelPath = faceEngine['flandmark-model-path'],
		displayImageFormat = faceEngine['original-image-format'],
		searchImageFormat = faceEngine['trained-image-format'],
		// profileImageFormat = faceEngine['profile-image-format'],
		maxConcurrent = faceEngine['max-concurrent'] || 1,
		s3Config = app.config.s3,
		veritoneApiClient = new VeritoneApi(app.config['veritone-api']),
		faceRecognitionClient = new FaceRecognitionClient(app.config['face-recognition']);

	var waitQueue = new Queue(maxConcurrent);

	setInterval(function checkProcess() {
		if (!waitQueue.isEmpty() && waitQueue.canRunNext()) {
			var payload = waitQueue.nextAvailable();
			// task running
			runningTask(payload);
			analyzeVideo(payload, function(err, taskOutput) {
				if (err) {
					// task err
					console.error('err', err);
					failTask(payload, err);
				} else {
					// task complete
					completeTask(payload, taskOutput);
				}

				var taskId = payload.taskId;
				 // deleteFiles(taskId, function(err) {
				 // 	if (err) {
				 // 		console.error('Error removing directory for taskId:', taskId, 'err:', err);
				 // 	}

				 // 	waitQueue.popFront();
				 // });
			});
		}
	}, 1000);

	app.get('/facedetect/health', function(req, res) {
		var status = {
			inLine: waitQueue.size - 1,
			totalProcessed: waitQueue.totalProcessed
		};

		res.status(200).send(status);
	});

	app.post('/facedetect/video', function processVideo(req, res) {
		if (!req.body.applicationId) {
			res.status(500).send('request must provide applicationId');
			return;
		}
		if (!req.body.recordingId) {
			res.status(500).send('request must provide recordingId');
			return;
		}
		if (!req.body.jobId) {
			res.status(500).send('request must provide jobId');
			return;
		}
		if (!req.body.taskId) {
			res.status(500).send('request must provide taskId');
			return;
		}

		console.log('received recording:', req.body.recordingId, 'job:', req.body.jobId);

		waitQueue.pushBack(req.body);

		var respBody = {
			taskEngine: 'task-face-clustering-server',
    		taskEngineId: uuid.v4()
		};

		res.status(202).send(respBody);
	});

	function runningTask(payload) {
		veritoneApiClient.updateTask(payload.jobId, payload.taskId, {
			taskStatus: 'running'
		}, function updateTaskRunningCallback(err) {
			if (err) {
				console.error('jobId:', payload.jobId, 'updateTaskRunning.error:', err);
			} else {
				console.log('jobId:', payload.jobId, 'updateTaskRunning.success');
			}
		});
	}

	function completeTask(payload, taskOutput) {
		veritoneApiClient.updateTask(payload.jobId, payload.taskId, {
			taskStatus: 'complete',
			taskOutput: taskOutput
		}, function updateTaskCompleteCallback(err) {
			if (err) {
				return failTask(payload, err);
			}

			console.log('jobId:', payload.jobId, 'updateTaskToComplete.success');
		});
	}

	function failTask(payload, err) {
		console.error(err);

		veritoneApiClient.updateTask(payload.jobId, payload.taskId, {
			taskStatus: 'failed', 
			taskOutput: err.message
		}, function failTaskCallback(err) {
			if (err) {
				console.error('jobId:', payload.jobId, 'updateTaskToFailed.error:', err);
			} else {
				console.log('jobId:', payload.jobId, 'updateTaskToFailed.success');
			}
		});
	}

	function deleteFiles(taskId, callback) {
		var dirPath = path.join(outputPath, taskId);
		rimraf(dirPath, callback);
	}

	function analyzeVideo(payload, callback) {
		var taskData = {
			applicationId: payload.applicationId,
			recordingId: payload.recordingId,
			jobId: payload.jobId,
			taskId: payload.taskId,
			taskOutputPath: path.join(outputPath, payload.taskId)
		};

		taskData.fileKeyPath = path.join(taskData.applicationId, taskData.recordingId, taskData.jobId);

		var taskOutput = {
			series: [],
			metadata: {}
		};

		async.waterfall([
				//STEP 1 - GET THE ASSET
				function(callback) {
					downloadAsset(taskData, function(err) {
						console.error(err);
						callback(err);
					});
				},
				//STEP 2 - EXTRACT FRAMES
				function(callback) {
					extractKeyFrames(taskData, function(err) {
						callback(err);
					});
				},				
				//STEP 3 - RUN THE PROCESS
				function(callback) {
					runFaceEngine(taskData, function(err) {
						callback(err);
					});
				},
				//STEP 4 - UPLOAD TO S3
				function(callback) {
					uploadImagesToS3(taskData.videoOutput, taskData, function(err) {
						callback(err);
					});
				},
				//STEP 5 - CALL RECOGNIZE
				function(callback) {
					recognize(taskData.videoOutput, taskData, function(err) {
						callback(err);
					})
				},
				//STEP 6 - CREATE NEW FACE SETS
				function(callback) {
					createNewFacesets(taskData.videoOutput, taskData, function(err) {
						callback(err);
					})
				}	,			
				//STEP 7 - GENERATE TASK OUTPUT
				function(callback) {
					taskOutput.series = generateTimeSeries(taskData.videoOutput, taskData);	
					callback(null);				
				}	
			],function(err) {
				console.log('error:');
				console.log(err);
				if(err) 
					return callback(err);
				callback(null, taskOutput);

		});
	}

	function recognize(faceSets, taskData, callback) {
		async.each(faceSets, function(faceSet, callback) {
			faceSet.status = 'pending';
			callback(null);
		}, function(err) {
			callback(err);
		});
	}


	function createNewFacesets(faceSets, taskData, callback) {
		async.each(faceSets, function(faceSet, callback) {
			if(faceSet.status !== 'pending')
				return callback(null);
			faceSet.faceSetId = uuid.v4();
			var newFaceSet = {
				"veritone-permissions": {
			        "ownerApplicationId": taskData.applicationId,
			        "acls": [
			            {
			                "id": taskData.applicationId,
			                "permission": {}
			            }
			        ]
			    },
				faceSetId: faceSet.faceSetId,
				status: 'pending',
				faces: []
			};
			_.each(faceSet.appearances, function(appearance){
				_.each(appearance.faces, function(face){
					if (!newFaceSet.profileImage) {
						newFaceSet.profileImage = path.join(taskData.fileKeyPath,face.originalImage);
					}
					newFaceSet.faces.push({
						faceId:uuid.v4(),
						trainingImage: path.join(taskData.fileKeyPath,face.trainingImage),
						originalImage: path.join(taskData.fileKeyPath,face.originalImage)
					});
				});
			});	

			console.log(newFaceSet);

			faceRecognitionClient.createFaceSet(newFaceSet, function(err, results) {
				if (err) {
					return callback(err);
				}
				callback(null);
			});
		}, function(err) {
			callback(err);
		});
	}

	//All trained and original images are uploaded.
	function uploadImagesToS3(faceSets, taskData, callback) {
		var allImages = [];
		_.each(faceSets, function(faceSet){
			_.each(faceSet.appearances, function(appearance){
				_.each(appearance.faces, function(face){
					allImages.push(face.originalImage);
					allImages.push(face.trainingImage);
				});
			});
		});

		console.log('Uploading ' + allImages.length + ' images.');

		var uploadQ = async.queue(function (image, callback) {
			var localFile = path.join(taskData.taskOutputPath, image);
			var fileKey = path.join(taskData.fileKeyPath, image);
			var uploadOptions = {
				localFile: localFile,
				bucket: s3Config.bucket,
				fileKey: fileKey,
				contentType: 'image/png'
			};
			s3.uploadFile(uploadOptions, callback);
		}, 20);

		uploadQ.drain = function(err) {
	    	console.log('Images have been uploaded.');
		    callback(err);
		}

		uploadQ.push(allImages);
	}


	function runFaceEngine(taskData, callback) {
		var initialFrame = 0;
		var finalFrame = 0;

		var appearanceMetaDataPath = path.join(taskData.taskOutputPath, appearanceMetaDataFileName);

		var commands = [faceEnginePath, taskData.taskOutputPath+'/frames', classifierPath, flandmarkModelPath, initialFrame, finalFrame, taskData.metadata.details.video.avgFrameRate, fps, taskData.taskOutputPath, appearanceMetaDataPath,'true'];
		var execCommand = commands.join(' ');
		console.log('executing: ' + execCommand);

		exec(execCommand, function(err, stdout, stderr) {
			if (err) {
				console.error('err:', err);
				console.error('stderr:', stderr);
				callback(err);
				return;
			}
			if (stderr) {
				console.error('stderr:', stderr);
				callback(stderr);
				return;
			}

			var response;
			try {
				response = JSON.parse(stdout);
			} catch(e) {
				console.error('error parsing json:', e);
				callback(e);
				return;
			}

			if (response.status !== 'ok') {
				console.error('error: ', response);
				callback(response);
				return;
			}

			fs.readFile(response.appearanceMetaDataPath, function (err, data) {
				if (err) {
					console.error('error reading appearanceMetaData:', err);
					callback(err);
					return;
				}

				try {
					taskData.videoOutput = JSON.parse(data);
					callback(null);
				} catch(e) {
					console.error('error parsing appearanceMetaData:', e);
					callback(e);
					return;
				}
			});
		});
	}

	function extractKeyFrames(taskData, callback) {

		if(!taskData) {
			callback('taskData is required!');
		}

		fs.mkdir(taskData.taskOutputPath+'/frames', function(err) {
			if (err) {
				console.log('Ignoring error when creating /frames directory:', err);
			}

			var initialFrame = 0;
			var finalFrame = 0;

			var appearanceMetaDataPath = path.join(taskData.taskOutputPath, appearanceMetaDataFileName);

			//ffmpeg -i test.flv -vf fps=1/600 thumb%04d.bmp
			var commands = [ffmpeg.ffmpegExecutable, '-loglevel fatal -i', taskData.videoPath, '-vf fps='+fps, path.join(taskData.taskOutputPath, '/frames/out%09d.bmp')];
			var execCommand = commands.join(' ');
			console.log('executing: ' + execCommand);

			exec(execCommand, function(err, stdout, stderr) {
				if (err) {
					console.log('in error1');
					console.error('err:', err);
					console.error('stderr:', stderr);
					callback(err);
					return;
				}
				if (stderr) {
					console.log('in error2');
					console.error('stderr:', stderr);
					callback(stderr);
					return;
				}


				ffmpeg.getMediaDetails(taskData.videoPath, function getMediaDetailsCallback(err, details) {
					if (err) {
						return callback(err);
					}

					console.log(details);

					if((details && details.video && details.video.avgFrameRate)) {
						console.log('ok');
						taskData.metadata = {};
						taskData.metadata.details = details;
						return callback(null);
					} else {
						return callback('Average frame rate not found.');
					}
				});
			});
		});
	}

	function downloadAsset(taskData, callback) {
		veritoneApiClient.getRecording(taskData.recordingId, function(err, response) {
				if (err) {
					callback(err);
					return;
				}

				var recording = response;

				if (!recording.mediaAsset) {
					callback('recording has no mediaAsset!');
					return;
				}

				// create tmp directory for the task
				// ignore errors, as we don't care if the dir already exists
				// (it shouldn't, but we still don't want to fail)
				fs.mkdir(taskData.taskOutputPath, function(err) {
					if (err) {
						console.log('Ignoring error when creating', taskData.taskOutputPath, 'output directory:', err);
					}

					// download video
					var videoUrl = recording.mediaAsset._uri;
					var videoName = path.basename(videoUrl);
					taskData.videoPath = path.join(taskData.taskOutputPath, videoName);

					veritoneApiClient.saveAssetToFile(recording.recordingId, recording.mediaAsset.assetId, taskData.videoPath, function(err) {
						if (err) {
							console.error('unable to download video');
							callback(err);
							return;
						}
						callback(null);
					});
				});
			});
	}

	function generateTimeSeries(faceSets, taskData) {
		var series = [];
		faceSets.forEach(function(faceSet) {
			faceSet.appearances.forEach(function(appearance) {
					if (appearance.faces.length >= 1 && appearance.range) {
						var face = appearance.faces[0];
						var appearanceObj = {
							faceSetId: faceSet.faceSetId,
							start: appearance.range.start,
							end: appearance.range.end,
							status: faceSet.status,
							originalImage: path.join(taskData.fileKeyPath, face.originalImage),
							profileImage: path.join(taskData.fileKeyPath, face.originalImage),
							trainingImage: path.join(taskData.fileKeyPath, face.trainingImage)
						};
						series.push(appearanceObj);
					}
			});
		});
		return series;
	}
};
