var request = require('request');
var fs = require('fs');
var mkdirp  = require('mkdirp');
var fileExists = require('file-exists');
var exec = require('child_process').exec;

var retry_delay = 10000;
var next_delay = 0;
var server_url = "http://localhost:3001";

var request_task = function()
{
    var await_task = function(callback)
    {
        console.log();
        console.log("Awainting work...");
        request(server_url + "/awaitTask", function(err, response, body)
        {
            if(err)
            {
                console.error("Got error awaiting task");
                console.error(err);
                console.error(body);
                setTimeout(request_task, retry_delay);
                return;
            }
            callback();
        });
    }

    var get_task = function()
    {
        console.log("Downloading work...");
        request(server_url + "/requestTask", function(err, response, body)
        {
            if(err)
            {
                console.error("Got error requesting task");
                console.error(err);
                console.error(body);
                setTimeout(request_task, retry_delay);
                return;
            }
            else if(response.statusCode != 200)
            {
                console.warn("Got non 200 status code");
                console.warn(body);
                setTimeout(request_task, next_delay);
                return;
            }
            else
            {
                var json;
                try
                {
                    json = JSON.parse(body);
                }
                catch(err)
                {
                    console.error("Unable to parse JSON");
                    console.error(err);
                    console.error(body);
                    setTimeout(request_task, retry_delay);
                    return;
                }
                prepare_dtw(json);
            }
        });
    }

    await_task(function()
    {
        get_task();
    });
}


var work_folder   = 'work';
// Ensure that folders are created
var folder_callback = function(err) { if(err) console.error(err); };
mkdirp(work_folder, folder_callback);

var write_file = function(filename, contents, callback)
{
    fs.writeFile(filename, contents, function(err) 
    {
        if(err) 
        {
            console.error("Unable to write ", filename);
            console.error(err);
            setTimeout(request_task, retry_delay);
            return;
        }
        else
        {
            callback();
        }
    }); 
}

var knn_executable = './tools/clf.run';
var run_knn_dtw = function(query, reference, dtw_args, callback)
{
    var command = knn_executable + ' --query_filename=' + query + ' --reference_filename=' + reference + ' ' + dtw_args;
    console.log("Running", command);
    exec(command,
        {maxBuffer: Number.POSITIVE_INFINITY},
            callback);
}

var prepare_dtw = function(task)
{
    var acquire_file = function(url, fileidentifier, callback)
    {
        console.log("Downloading", fileidentifier);
        request(server_url + '/' + url, function(err, response, body)
        {
            if(err || response.statusCode != 200)
            {
                console.error("Unable to download " + fileidentifier + "-file");
                console.error(err);
                console.error(body);
                setTimeout(request_task, retry_delay);
                return;
            }
            else
            {
                callback(body);
            }
        });
    }

    var send_response = function(result)
    {
        var options = {
            uri: server_url + '/replyTask',
            method: 'POST',
            json: {
                name: task.name,
                query: task.query,
                result: result
            }
        };

        request(options, function(err, response, body) 
        {
            if (err || response.statusCode != 200) 
            {
                console.error("Unable to upload response");
                console.error(err);
                console.error(body);
                setTimeout(request_task, retry_delay);
                return;
            }
            else
            {
                console.log("Succesfully uploaded a piece of work!");
                setTimeout(request_task, next_delay);
                return;
            }
        });
    }

    var run_knn = function(query_path, reference_path, dtw_args)
    {
        run_knn_dtw(query_path, reference_path, dtw_args, function(err, result)
        {
            if(err)
            {
                console.error("Error running knn-dtw");
                console.error(err);
                setTimeout(request_task, retry_delay);
                return;
            }
            else
            {
                var json;
                try
                {
                    json = JSON.parse(result);
                }
                catch(err)
                {
                    console.error("Unable to parse knn-dtw response");
                    console.error(err);
                    console.error(result);
                    setTimeout(request_task, retry_delay);
                    return;
                }
                send_response(json);
            }
        });
    }

    console.log("Got work:", task.name, "part:", task.part);
    var dtw_args = task.dtw_args || "";

    acquire_file(task.query, "query", function(query)
    {
        var query_path = work_folder + '/' + task.name + "-QUERY";
        write_file(query_path, query, function()
        {
            var reference_path = work_folder + '/' + task.name + "-REFERENCE";
            if(fileExists(reference_path))
            {
                console.log("Using cached reference");
                run_knn(query_path, reference_path, dtw_args);
            }
            else
            {
                acquire_file(task.reference, "reference", function(reference)
                {
                    write_file(reference_path, reference, function()
                    {
                        run_knn(query_path, reference_path, dtw_args);
                    });
                });
            }
        });
    });
}

request_task();
