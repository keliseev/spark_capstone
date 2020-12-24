# BigData Marketing Analytics

Provides analysis based on two datasets: click stream representing 
user activity and purchases statistic.

### Prerequisites

* Oracle Java 8+
* IDE IntelliJ IDEA
* For the SBT usage, the IDE should have [SBT plugin](https://plugins.jetbrains.com/plugin/5007-sbt)
## Executing
In IDE only at the moment. Can be compiled
```bash
sbt compile
```
runned
```bash
sbt run
```
and packaged into JAR
```bash
sbt package
```

## Usage
After placing .gz in resource folder invoke 
```scala
import capstone.dao.ProjectionsDAO

val projectionsDao: ProjectionsDAO = new ProjectionsDAO()
projectionsDao.refreshProjections()
```
for .parquet files with purchases attribution projection being updated.
The statistics methods for marketing channels or marketing campaigns can
be executed
```scala
import capstone.dao.ProjectionsDAO
import capstone.analyzers.{CampaignsAnalyzer, ChannelsAnalyzer}

val projectionsDao: ProjectionsDAO = new ProjectionsDAO()
projectionsDao.refreshProjections()

val campaignsAnalyzer: CampaignsAnalyzer = new CampaignsAnalyzer(projectionsDao)
val channelsAnalyzer: ChannelsAnalyzer = new ChannelsAnalyzer(projectionsDao)

campaignsAnalyzer.showTopProfitableCampaignsAPI()
channelsAnalyzer.showTopChannelsAPI()
```
Result will provide respectively the Top 10 marketing campaigns that bring the 
biggest revenue, and the most popular channel that drives the highest amount of 
unique sessions in each campaign.

## Contributing
Pull requests are most welcome.