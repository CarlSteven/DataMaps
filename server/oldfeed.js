//required packages
var csvmodule = Meteor.npmRequire('csv');
var fs = Meteor.npmRequire('fs');
var pathModule = Meteor.npmRequire('path');

var oldWatcher = chokidar.watch('/hnet/better/current', {
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
        logger.info('Ready for changes in /hnet/better/2015/.');
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
                        parsedLine["HNET_AA_" + key.replace(/mt_|jf_/i, "").replace("1min_", "")] = parsedLine[key];
                        delete parsedLine[key];
                    } else if (key.toString() == "TIMESTAMP") {
                        //Need to handle timestamp -> TheTime conversion for one minute data
                        // NOT CORRECT YET - TODO!!
                        parsedLine["TheTime"] = ((((new Date(parsedLine["TIMESTAMP"]).valueOf() + 25569) / 8640) - 6) / 3600);
                    }

                }
            });
            batchLiveDataUpsert(parsedLines, path);
        });
    });
});