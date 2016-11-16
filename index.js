var request = require('request');

var retry_delay = 10000;
var next_delay = 0;
var server_url = "http://localhost:3001";

var request_task = function()
{
    var await_task = function(callback)
    {
        console.log("Awainting work...");
        request(server_url + "/awaitTask", function(err, response, body)
        {
            if(err)
            {
                console.error("Got error awaiting task");
                console.error(err);
                console.error(body);
                setTimeout(request_task, retry_delay);
            }
            callback();
        });
    }

    var get_task = function()
    {
        request(server_url + "/requestTask", function(err, response, body)
        {
            if(err)
            {
                console.error("Got error requesting task");
                console.error(err);
                console.error(body);
                setTimeout(request_task, retry_delay);
            }
            else if(response.statusCode != 200)
            {
                console.warn("Got non 200 status code");
                console.warn(body);
                setTimeout(request_task, retry_delay);
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

var mkdirp  = require('mkdirp');

var work_folder   = 'work';
// Ensure that folders are created
var folder_callback = function(err) { if(err) console.error(err); };
mkdirp(work_folder, folder_callback);

var fs = require('fs');
var write_file = function(filename, contents, callback)
{
    fs.writeFile(filename, contents, function(err) 
    {
        if(err) 
        {
            console.error("Unable to write ", filename);
            console.error(err);
            setTimeout(request_task, retry_delay);
        }
        else
        {
            callback();
        }
    }); 
}

var exec = require('child_process').exec;
var knn_executable = './tools/clf.run';
var run_knn_dtw = function(query, reference, knn, callback)
{
    exec(knn_executable + ' --query_filename=' + query + ' --reference_filename=' + reference + ' --knn=' + knn, callback);
}

var prepare_dtw = function(task)
{
    console.log(task);

    var acquire_file = function(url, fileidentifier, callback)
    {
        request(server_url + '/' + url, function(err, response, body)
        {
            if(err || response.statusCode != 200)
            {
                console.error("Unable to download " + fileidentifier + "-file");
                console.error(err);
                console.error(body);
                setTimeout(request_task, retry_delay);
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
            }
            else
            {
                console.log("Succesfully uploaded a piece of work!");
                setTimeout(request_task, next_delay);
            }
        });
    }

    acquire_file(task.query, "query", function(query)
    {
        var query_path = work_folder + '/' + task.name + "-QUERY";

        // TODO: Only download reference is not yet available
        acquire_file(task.reference, "reference", function(reference)
        {
            var reference_path = work_folder + '/' + task.name + "-REFERENCE";
            write_file(query_path, query, function()
            {
                write_file(reference_path, reference, function()
                {
                    run_knn_dtw(query_path, reference_path, task.knn, function(err, result)
                    {
                        if(err)
                        {
                            console.error("Error running knn-dtw");
                            console.error(err);
                            setTimeout(request_task, retry_delay);
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
                                setTimeout(request_task, retry_delay);
                                return;
                            }
                            send_response(json);
                        }
                    });
                });
            });
        });
    });
}

request_task();
