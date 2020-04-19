

const getDisplayResults = async (path: string, username: string): Promise<SfdxDisplayResult> =>
    (await exec2JSON(`sfdx force:org:display -u ${username} --json`, { cwd: path })).result as SfdxDisplayResult;


