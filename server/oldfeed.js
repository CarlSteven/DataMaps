//required packages
var csvmodule = Meteor.npmRequire('csv');
var fs = Meteor.npmRequire('fs');
var pathModule = Meteor.npmRequire('path');

var oldWatcher = chokidar.watch('/hnet/incoming/betterData', {
    ignored: /[\/\\]\./,
    ignoreInitial: true,
    usePolling: true,
    persistent: true
});

oldWatcher
    .on('add', function (path) {
        logger.info('File ', path, ' has been added.');
        readOldFile(path);
    })
    .on('change', function (path) {
        logger.info('File', path, 'has been changed');
        readFile(path);
    })
    .on('addDir', function (path) {
        logger.info('Directory', path, 'has been added');
    })
    .on('error', function (error) {
        logger.error('Error happened', error);
    })
    .on('ready', function () {
        logger.info('Ready for changes in /hnet/incoming/betterData/.');
    });

var readOldFile = Meteor.bindEnvironment(function (path) {
    fs.readFile(path, 'utf-8', function (err, output) {
        if (path.indexOf("1min") > -1) {
            var outputArr = output.split('\n');
            delete outputArr[0];
            delete outputArr[2];
            delete outputArr[3];
            var outTemp = "";
            outputArr.forEach(function (outLine) {
                outTemp += outLine + "\n";
            });
            output = outTemp;
        }
        csvmodule.parse(output, {
            auto_parse: true,
            columns: true
        }, function (err, parsedLines) {
            if (err) {
                logger.error(err);
            }
            parsedLines.forEach(function (parsedLine) {
                for (var key in parsedLine) {
                    if (parsedLine.hasOwnProperty(key) && key.toString() != "TheTime" && key.toString() != "TIMESTAMP") {
                        parsedLine["HNET_AA_" + key.replace(/mt_|jf_/i, "").replace("1min_", "").replace("_Avg", "Avg").replace("_ms_2", "_ms2").replace("za_", "za").replace("sp_", "sp").replace("ps_", "ps")] = parsedLine[key];
                        delete parsedLine[key];
                    }

                }
            });
            batchLiveDataUpsert(parsedLines, path);
        });
    });
});

var batchLiveDataUpsert = Meteor.bindEnvironment(function (parsedLines, path) {
    //find the site information
    var pathArray = path.toString().split(pathModule.sep);
    var parentDir = pathArray[pathArray.length - 2];
    var site = Monitors.find({incoming: parentDir}).fetch()[0];
    if (site.AQSID) {
        var allObjects = [];
        for (var k = 0; k < parsedLines.length; k++) {
            var singleObj = makeObj(parsedLines[k]); //add data in
            if (parsedLines[k].TheTime) {
                var epoch = ((parsedLines[k].TheTime - 25569) * 86400) + 6 * 3600;
                epoch = epoch - (epoch % 1); //rounding down
                singleObj.epoch = epoch;
                singleObj.epoch5min = epoch - (epoch % 300);
                singleObj.theTime = parsedLines[k].TheTime;
            } else {
                singleObj.TIMESTAMP = parsedLines[k].TIMESTAMP;
                singleObj.epoch = moment(parsedLines[k].TIMESTAMP, "YYYY-MM-DD HH:mm:ss").unix();
            }
            singleObj.site = site.AQSID;
            singleObj.file = pathArray[pathArray.length - 1];
            singleObj._id = site.AQSID + '_' + singleObj.epoch;
            allObjects.push(singleObj);
        }

        //using bulCollectionUpdate
        bulkCollectionUpdate(LiveData, allObjects, {
            callback: function () {
                logger.info('LiveData updated for : ', site.AQSID, 'Done with bulk data update.');
            }
        });
    }
});

var makeObj = function (keys) {
    var obj = {};
    obj.subTypes = {};
    var metron = [];
    for (var key in keys) {
        if (keys.hasOwnProperty(key)) {
            var subKeys = key.split('_');
            if (subKeys.length > 1) { //skipping 'TheTime'
                var alphaSite = subKeys[0] + '_' + subKeys[1];
                var metric = subKeys[subKeys.length - 1]; //i.e. conc., direction, etc.
                var metrized = key.replace(alphaSite + '_', '');
                metron = metrized.replace('_' + metric, ''); //wind, O3, etc.
                var val = keys[key];
                if (!obj.subTypes[metron]) {
                    obj.subTypes[metron] = [{
                        metric: metric,
                        val: val
                    }];
                } else {
                    if (metric === 'Flag') { //Flag should be always first
                        obj.subTypes[metron].unshift({
                            metric: metric,
                            val: val
                        });
                    } else {
                        obj.subTypes[metron].push({
                            metric: metric,
                            val: val
                        });
                    }
                }
            }
        }
    }
    return obj;
};
