const fs = require('fs');
const dns = require('dns');

// JSON Distributed Virtual FileSystem
const ENV_PREFIX = 'JDVFS_';
const DEFAULT_CONF_FILE = '/etc/jdvfs.conf';
const DEFAULT_HOST = '127.0.0.1';
const DEFAULT_HOST_FAMILY = 'ipv4';
const DEFAULT_PORT = 1234;

async function parse_address(addr)
{
    addr = ((addr || '') +'').trim();
    
    if(!addr) return null;
    
    var found = false;
    
    // check for path:
    var path = '';
    addr.replace(/([0-9a-z.-]+|\[[0-9a-f:]+\]|^)(:[0-9]+|)(([A-Z]?:|\\\\|\/).*?|)$/gi, ($0, $1, $2, $3) => {
        path = $3 || path;
        if(path)
        {
            path = path.replace(/^:/g, ''); // remove colon-prefix (but starting with / or A-Z: or \\ is also valid for linux/windows systems absolute paths)
            found = true;
        }
    });
    
    // check for port:
    var port = DEFAULT_PORT;
    addr.replace(/([0-9a-z.-]+|\[[0-9a-f:]+\]|^):([0-9]+)(([A-Z]?:|\\\\|\/).*?|)$/gi, ($0, $1, $2) => {
        port = parseInt($2) || port;
        found = true;
    });
    
    // check for host:
    var host = DEFAULT_HOST;
    var family = DEFAULT_HOST_FAMILY;
    
    // if ipv4:
    addr.replace(/^([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)(:[0-9]+|)(([A-Z]?:|\\\\|\/).*?|)$/gi, ($0, $1) => {
        host = $1 || host;
        found = true;
    });
    
    // if ipv6:
    addr.replace(/^(\[([0-9a-f:]+)\])(:[0-9]+|)(([A-Z]?:|\\\\|\/).*?|)$/gi, ($0, $1, $2, $3) => {
        host = $2 || $3 || host;
        family = 'ipv6';
        found = true;
    });
    
    // if hostname:
    var hostname = '';
    addr.replace(/^([a-z0-9][a-z0-9.-]*)(:[0-9]+|)(([A-Z]?:|\\\\|\/).*?|)$/gi, async ($0, $1) => {
        hostname = $1 || hostname;
        host = ''; // pending to be resolved, if empty, we could not resolve, and it must be tried again at a later stage
        found = true;
    });
    if(hostname)
    {
        // resolve hostname to an address
        var results = await dns.promises.lookup(hostname, {family: 0, all: true, verbatim: true}).catch(function(err) {});
        
        if(results)
        {
            for(var i=0;i<results.length;++i)
            {
                var result = results[i];
                
                if(!result.address) continue;
                
                // check if the given port is online (cycle through addresses until port that is online is found)
                // TODO: return either the first address, or the first address that is online (with the given port)
                
                host = result.address;
                if(result.family === 6)
                {
                    family = 'ipv6';
                }
                else if(result.family === 4)
                {
                    family = 'ipv4';
                }
                
                break;
            }
        }
    }
    
    if(!found) return null;
    
    return {
        hostname: hostname || host,
        address: host,
        family: family,
        port: port,
        path: path
    };
}

async function parse_node_line(ln)
{
    if(!ln) return null;
    
    // fix whitespaces
    ln = ln.trim();
    
    // detect empty string
    if(!ln) return null;
    
    // first assume JSON
    try
    {
        return JSON.parse(ln);
    }
    catch(err) {}
    
    // then assume a list of host:port combinations (domain name, ipv4, or ipv6)
    var list = ln.split(/[,;\t\s ]+/g);
    var nodes = [];
    
    for(var i=0;i<list.length;++i)
    {
        ln = list[i].trim();
        
        if(!ln) continue;
        
        var addr = await parse_address(ln);
        
        if(!addr) continue;
        
        nodes.push({
            address: addr,
            socket: null
        });
    }
    
    return nodes;
}

async function parse_config_file(file)
{
    var cfg = null;
    
    var filedata = await fs.promises.readFile(file, {encoding: 'utf8'}).catch(function(err)
    {
        // only report error if not default conf file, which may be left out
        if(file !== DEFAULT_CONF_FILE)
        {
            console.error(err);
        }
    });
    if(filedata)
    {
        try
        {
            cfg = JSON.parse(filedata);
        }
        catch(err) {}
        
        if(cfg === null)
        {
            // by default we assume all lines in the config file are nodes in the section [Nodes] (or [nodes] lowercase)
            // but other sections are possible, in that case, the next lines are ignored for nodes, unless [nodes] appears
            
            // try to split on separate lines
            var nodes = [];
            var lines = filedata.split(/\r?\n/g);
            for(var i=0;i<lines.length;++i)
            {
                var lines_i = lines[i];
                if(lines_i.startsWith('#')) continue; // skip commented lines
                
                var node = await parse_node_line(lines_i);
                if(node)
                {
                    if(Array.isArray(node))
                    {
                        for(var j=0;j<node.length;++j)
                        {
                            nodes.push(node[j]);
                        }
                    }
                    else
                    {
                        nodes.push(node);
                    }
                }
            }
            
            if(nodes.length)
            {
                cfg.nodes = nodes;
            }
        }
    }
    
    return cfg;
}

module.exports = {
    parse_address: parse_address,
    create: async function(options)
    {
        options = options || {};
        
        const cfg = {
            nodes: []
        };
        
        
        // parse from environment variables for default settings override
        var env = options.env || process.env || {};
        
        if(typeof env[ENV_PREFIX + 'NODES'] === 'string')
        {
            var envNodes = await parse_node_line(env[ENV_PREFIX + 'NODES']);
            
            if(envNodes)
            {
                cfg.nodes = envNodes;
            }
        }
        
        
        // parse from command-line arguments for secondary default settings override
        var args = options.argv || process.argv || [];
        
        var argNodes = null;
        for(var i=1;i<args.length;++i)
        {
            var arg = args[i];
            
            if(arg === '--nodes')
            {
                argNodes = argNodes || [];
                
                while(++i < args.length)
                {
                    arg = args[i];
                    
                    if(arg.startsWith('--')) break;
                    
                    var _argNodes = await parse_node_line(arg);
                    
                    for(var j=0;j<_argNodes.length;++j)
                    {
                        argNodes.push(_argNodes[j]);
                    }
                }
            }
            else if(arg === '--')
            {
                break;
            }
        }
        if(argNodes !== null)
        {
            cfg.nodes = argNodes;
        }
        
        
        // parse from file for settings
        var cfgFile = options.configFile || DEFAULT_CONF_FILE;
        
        if(cfgFile)
        {
            var parsedConfig = await parse_config_file(cfgFile);
            
            if(parsedConfig)
            {
                if(parsedConfig.nodes)
                {
                    cfg.nodes = parsedConfig.nodes;
                }
            }
        }
        
        
        // add special localNode to nodes from settings, regardless of configuration, always add this
        if(options.localNode)
        {
            cfg.nodes.unshift(options.localNode);
        }
        
        
        // return config object for ease of access to properties, and reload the configuration on the fly
        const self = {
            _configFile: cfgFile,
            _config: cfg,
            get_nodes: function()
            {
                return this._config.nodes;
            },
            reload: async function()
            {
                // reload configuration from file
                if(cfgFile)
                {
                    var parsedConfig = await parse_config_file(cfgFile);
                    
                    if(parsedConfig)
                    {
                        if(parsedConfig.nodes)
                        {
                            cfg.nodes = parsedConfig.nodes;
                            
                            // add special localNode to nodes from settings, regardless of configuration, always add this
                            if(options.localNode)
                            {
                                cfg.nodes.unshift(options.localNode);
                            }
                        }
                    }
                }
            }
        };
        
        return self;
    }
};
