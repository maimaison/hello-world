import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.dalet.webservice.services.assetservice.definition.*
import com.dalet.webservice.services.categoryservice.definition.*
import com.dalet.webservice.services.configurationservice.definition.*
import com.dalet.webservice.services.errors.ResourceDoesNotExist
import com.dalet.webservice.services.errors.ResourcePreconditionViolation
import com.dalet.webservice.services.mediaservice.definition.*
import com.dalet.webservice.services.metadataservice.definition.*
import com.dalet.webservice.services.workflowservice.definition.*


SCRIPT_NAME = "Maintenance (Compliance)"
LOG = LoggerFactory.getLogger("Script - " + SCRIPT_NAME)
LOG.info(SCRIPT_NAME + " script started...")


/** CONFIGURATION **/
String message // Ending state of script for logger
/* Define output for this script */
class Output {
	String CLASS_NAME = this.class.name
	Logger LOG = LoggerFactory.getLogger("Class instance - $CLASS_NAME")
	com.dalet.bpm.engine.plugins.localcontext.ActivityLocalContext context
	def execution
	String taskResult // error, failure, noAction, success
	// map of error conditions
	def errorMap = [
		dataNotFound: "A data field that was assumed to be on this asset is missing.",
		generic: "Generic error. This error does not have a specific resolution.",
		invalidAssetType: "The asset calling this BPM is the wrong type.",
		invalidCategory: "The category specified does not exist or is invalid for this script.",
		invalidFormat: "The format of the input asset is not appropriate for this script.",
		invalidStorageUnit: "There is valid media on the title however it is not stored in a valid Storage Unit. The media must be physically transferred in Dalet to the correct storage unit before trying again.",
		noCategory: "There are no parent Categories given for the script to search in. An ID for at least one Category must be given to this script's stencil in the BPM.",
		noFormat: "There are no formats given for the script to match against. An ID for at least one video format must be given to this script's stencil in the BPM",
		noMedia: "There is no valid media attached to this title.",
		noTitleId: "The script was not supplied with an asset titleId. Possible causes: the BPM is not passing a valid ID, the asset that called the BPM is an incorrect type."
	]
	String errorMessage // String for outputting the errorMap key when needed
	String errorDescription // String for outputting the errorMap value when needed
	
    Output(){
		LOG.info(CLASS_NAME + " id " + System.identityHashCode(this) + " created...")
	}
	
	Output(def context, def execution){
		LOG.info(CLASS_NAME + " id " + System.identityHashCode(this) + " created. Inherited local context...")
		this.context = context
		this.execution = execution
	}
     
    Output(def context, def execution, String taskResult){
        LOG.info(CLASS_NAME + " id " + System.identityHashCode(this) + " created. Inherited local context...")
		this.context = context
		this.execution = execution
		this.taskResult = taskResult
		LOG.info("The input taskResult is $taskResult")
    }
	
	String send(String taskResult = "success", errorMessage = "generic"){
		this.taskResult = taskResult
		this.errorMessage = errorMessage
		String message
		switch(taskResult){
			case "error":
				message = "This script is ending in error."
				errorDescription = errorMap.get(errorMessage)
				break
			case "failure":
				message = "This script encountered a failure. No action will be taken."
				taskResult = "noAction"
				break
			case "noAction":
				message = "This script is ending with no action taken."
				taskResult = "noAction"
				break
			case "success":
				message = "This script is ending with success."
				break
			default:
				message = "This script encountered an error. The taskResult is not defined correctly. Setting taskResult to noAction to avoid the BPM taking an incorrect action."
				taskResult = "noAction"
				break			
		}

		List<String> filtered = ["class","active","CLASS_NAME","LOG","context","errorMap","execution"]
		if(taskResult == "success"){
			filtered << "errorMessage"
			filtered << "errorDescription"
		}
		this.properties.sort{it.key}.collect{it}.findAll{!filtered.contains(it.key)}.each{
			switch(it.value){
				case {it instanceof ArrayList}:
					if(it.value.size == 0){
						LOG.info("Output list " + it.key + " is empty.")
						context.setVariable(execution, it.key, "")
						}
					else{
						LOG.info("Output list " + it.key + " is " + it.value.join(","))
						context.setVariable(execution, it.key, it.value.join(","))
						}
					break
				case {it instanceof Boolean}:
					LOG.info("Output (Boolean)" + it.key + " is " + it.value.toString())
					context.setVariable(execution, it.key, it.value.toString())
					break
				default:
					LOG.info("Output " + it.key + " is " + it.value)
					context.setVariable(execution, it.key, it.value)
					break
			}
		}
		return message
	}
}

/* Define a simple method to move links or delete them between the subcategories */
def moveOrRemoveLink(long linkId, Category source, Category destination = null){
	boolean result = true // return value of API call
	String message
	
	try{
		result = daletAPI.AssetService().removeLinkFromCategory(linkId, source.id)
		LOG.debug("Removing link ID $linkId from category ID $source was successful. The return value is $result.")
		message = "Asset link ID $linkId was removed from category $source.name (ID: $source.id)"
	} catch (RuntimeException e){
		LOG.error("The attempt to remove link ID $linkId resulted in exception $e")
		result = false
		message = "The attempt to remove asset link ID $linkId from category $source.name (ID: $source.id) failed."
	}
	if(destination != null){
		try{
			daletAPI.AssetService().linkAssetToCategory(linkId,destination.id)
			LOG.debug("Adding link ID $linkId from category ID $destination was successful. The return value is $result.")
			result = true
			if(message != null){
				message = message + "\nAlso, "
			}
			message = message + "Asset link ID $linkId was added to $destination.name (ID: $destination.id)"
		} catch(RuntimeException e){
			LOG.error("The attempt to link ID $linkId resulted in exception $e")
			result = false
			if(message != null){
				message = message + "\nAlso, "
			}
			message = message + "The attempt to add asset link ID $linkId to category $destination.name (ID: $destination.id) failed."
		}
	}
	LOG.debug("This call to moveOrRemoveLink is ending. The return value is $result.")
	return [result, message]
}

/* Define a method to attempt to start a recovery workflow on an input title ID */
def runWorkflow(long linkId, String workflowName = workflowName){
	String message
	boolean result
	def thisWorkflowDefinition
	
	/* Check that the input workflowName is an actual workflow on the site */
	LOG.info("Method runWorklow initiated. The input linkId is $linkId and the input workflowName is $workflowName")
	if(workflowName != null){
		LOG.debug("Checking the string value given for the recovery workflow against all workflows on the site...")
		try{thisWorkflowDefinition = daletAPI.WorkflowService().getAllWorkflowDefinitions(true, true).findAll{it.name == workflowName}
			if(thisWorkflowDefinition.size < 1){
				LOG.warn("The input name $workflowName does not exist as a workflow on this site. No action will be taken.")
				return [result = false, message = "workflowDoesNotExist"]
			}
			else if(thisWorkflowDefinition.size > 1){
				LOG.warn("There are more than one workflow definitions for they name specified ($workflowName). Only one workflow should be defined per input given. No action will be taken.")
				return [result = false, message = "multipleWorkflows"]
			}
			else if(thisWorkflowDefinition.size == 1){
				LOG.debug("A workflow has been found for $workflowName. Its ID is $thisWorkflowDefinition.id")
			}
			else{
				LOG.error("An unknown error occurred.")
				return [result = false, message = "unknownError"]
			}
		} catch (RuntimeException e){
			LOG.error("An unexpected exception occurred. The exception is $e")
			return [result = false, message = "exception"]
		}	
	} else{
		LOG.info("The workflowName is null. Assuming there is no workflow to call so no action will be taken.")
		return [result = false, message = "noWorkflowGiven"]
	}
	LOG.debug("thisWorkflowDefinition.status is $thisWorkflowDefinition.status")
	if(thisWorkflowDefinition.status == [WorkflowDefinitionStatus.DISABLED]){
		LOG.warn("The workflow '$workflowName' is disabled on this site and cannot be run.")
		return [result = false, message = "workflowDisabled"]
	}
	else{
		LOG.debug("The workflow '$workflowName' is enabled on this site. Attempting to run...")
		String newWorkflowInstance
		try{newWorkflowInstance = daletAPI.WorkflowService().startWorkflowOnAsset(thisWorkflowDefinition[0].key, linkId)
			LOG.info("A new instance of $thisWorkflowDefinition.name was started on asset ID $linkId.")
		} catch (RuntimeException e){
			LOG.error("An unexpected exception occurred. The exception is $e")
			return [result = false, message = "exception"]
		}
	}
	return [result = true, message = "success"]
}

boolean result // generic reciever for success/fail information from API and methods
String resultText // generic reciever for API and method return text

/** INPUTS **/
long libraryId
long mainCategory
long maxNumber
String workflowName


/** EXECUTE **/
/* Set an object for each title ID to store all the output variables */
Output thisOutput = new Output(context,execution)

/* Test the input */
try{libraryId = [[libraryId]].toLong()
	LOG.info("The ID value given for this site's library category is $libraryId")
} catch (NumberFormatException e){
	LOG.error("No input library ID is given.")
	message = thisOutput.send("error", "noCategory") 
	LOG.info("$SCRIPT_NAME is ending. $message")
	return
}  catch (RuntimeException e){
	LOG.error("An unexpected exception occurred. The exception is $e")
	message = thisOutput.send("error", "generic") 
	LOG.info("$SCRIPT_NAME is ending. $message")
	return
}
try{mainCategory = [[mainCategory]].toLong()
	LOG.info("The value(s) given for the input 'mainCategory' is $mainCategory")
} catch (NumberFormatException e){
	LOG.error("No input category ID is given.")
	message = thisOutput.send("error", "noCategory") 
	LOG.info("$SCRIPT_NAME is ending. $message")
	return
}  catch (RuntimeException e){
	LOG.error("An unexpected exception occurred. The exception is $e")
	message = thisOutput.send("error", "generic") 
	LOG.info("$SCRIPT_NAME is ending. $message")
	return
}
try{maxNumber = [[maxNumber]].toLong()
	LOG.info("The value(s) given for the input 'maxNumber' is $maxNumber")
} catch (NumberFormatException e){
	LOG.info("No input maxNumber is given. Assuming a default of 20.")
	maxNumber = 20
}  catch (RuntimeException e){
	LOG.error("An unexpected exception occurred. The exception is $e")
	message = thisOutput.send("error", "generic") 
	LOG.info("$SCRIPT_NAME is ending. $message")
	return
}
try{workflowName = [[workflowName]]
	LOG.info("The value given for the input 'workflowName' is $workflowName")
} catch(NullPointerException e){
	LOG.warn("No input is given for the workflowName. This script will assume no recovery workflow is intended for use.")
	workflowName = ""
} catch (RuntimeException e){
	LOG.error("An unexpected exception occurred. The exception is $e")
	message = thisOutput.send("error", "generic") 
	LOG.info("$SCRIPT_NAME is ending. $message")
	return
}

/* Check that the libraryId is an actual category on the site */
def libraryCategory
try{libraryCategory = daletAPI.CategoryService().getCategory(libraryId)
	LOG.debug("Library category on this site is $libraryCategory")
} catch (RuntimeException e){
	LOG.error("An unexpected exception occurred. The exception is $e")
	message = thisOutput.send("error", "generic") 
	LOG.info("$SCRIPT_NAME is ending. $message")
	return
} catch (ResourcePreconditionViolation e){
	LOG.error("The Library category is in Recycled. The exception is $e")
	thisOutput.errorMap.put("libraryInRecycled", "The library ID input to this script ('libraryId') is currently in the recycling bin on this site. This script cannot function correctly as a result. Specify another category that is currently in use or resinstate the current category with this ID.")
	message = thisOutput.send("error", "libraryInRecycled") 
	LOG.info("$SCRIPT_NAME is ending. $message")
	return
} catch (ResourceDoesNotExist e){
	LOG.error("The Library category is in Recycled. The exception is $e")
	thisOutput.errorMap.put("noLibrary", "The library ID input to this script ('libraryId') does not exist on this site. This script cannot function correctly as a result. Specify a category that is currently in use.")
	message = thisOutput.send("error", "noLibrary") 
	LOG.info("$SCRIPT_NAME is ending. $message")
	return
}

/* Check that this category has the correct working subcategories (queued, processing, suspended, and failed) */
Category failed
Category processing
Category queued
Category suspended
long mainCategoryChildCount
def workingCategories

try{mainCategoryChildCount = daletAPI.CategoryService().getCategoriesByIds([mainCategory])[0].childCount.toLong()
	LOG.debug("mainCategoryChildCount is $mainCategoryChildCount")
}  catch (RuntimeException e){
	LOG.error("An unexpected exception occurred. The exception is $e")
	message = thisOutput.send("error", "generic") 
	LOG.info("$SCRIPT_NAME is ending. $message")
	return
}
if(!mainCategoryChildCount){
	LOG.warn("No valid categories returned during API call.")
	message = thisOutput.send("error", "invalidCategory") 
	LOG.info("$SCRIPT_NAME is ending. $message")
	return
}

try{workingCategories = daletAPI.CategoryService().getSubCategories(mainCategory,0.toLong(),mainCategoryChildCount)
} catch (RuntimeException e){
	LOG.error("An unexpected exception occurred. The exception is $e")
	message = thisOutput.send("error", "generic") 
	LOG.info("$SCRIPT_NAME is ending. $message")
	return
}

for(category in workingCategories){
	if(category.name == "queued"){
		queued = category
	}
	if(category.name == "processing"){
		processing = category
	}
	if(category.name == "failed"){
		failed = category
	}
	if(category.name == "suspended"){
		suspended = category
	}
}
LOG.debug("The ID for category '$queued.name' is $queued.id. The ID for category '$processing.name' is $processing.id. The ID for category '$failed.name' is $failed.id. The ID for category '$suspended.name' is $suspended.id.")
if(queued != null && processing != null && failed != null && suspended != null){
	LOG.info("The main category specified has the correct working subcategories.")
} else{
	LOG.warn("The main category specified does not have the correct working subcategories.")
	thisOutput.errorMap.put("noSubCategories", "The three working categories: queued, processing, suspended, and failed do not all exist in the main category specified in this script. These must be added to the category manually before this script can run.")
	LOG.debug("New errorMap is $thisOutput.errorMap")
	message = thisOutput.send("error", "noSubCategories") 
	LOG.info("$SCRIPT_NAME is ending. $message")
	return
}

/* Check if the queued subcategory has priority categories within it. If it does note their presence with a boolean marker and capture those categories' IDs */
Category high
Category low
Category normal
Category urgent
def theseCategories = []
def priorityCategories = []

if(queued.childCount > 0){
	try{
		theseCategories = daletAPI.CategoryService().getSubCategories(queued.id,0.toLong(),queued.childCount)
	} catch (RuntimeException e){
		LOG.error("An unexpected exception occurred. The exception is $e")
		message = thisOutput.send("error", "generic") 
		LOG.info("$SCRIPT_NAME is ending. $message")
		return
	}
	for(category in theseCategories){
		boolean found
		if(category.name == "urgent"){
			urgent = category
			found = true
			priorityCategories.add(category)
		}
		if(category.name == "high"){
			high = category
			found = true
			priorityCategories.add(category)
		}
		if(category.name == "normal"){
			normal = category
			found = true
			priorityCategories.add(category)
		}
		if(category.name == "low"){
			low = category
			found = true
			priorityCategories.add(category)
		}
		if(found){
			LOG.info("The priority category $category.name was found. It's ID is $category.id")
		}
	}
} else{
	LOG.info("The queued category has no subcategories. All links will therefore be treated with equal priority.")
}
priorityCategories.add(queued)

/* Get the assets in the process category and examine them. There is a limit of 500 assets per call so if the number is greater than 500 then a routine to page the total by 500 per page will run up to a maximum of 10,000 assets. If there are more than 10,000 assets then the script will err. */
long numberOfProcessingItems
def theseAssets = []
try{numberOfProcessingItems = daletAPI.CategoryService().getCategoriesByIds([processing.id])[0].itemsCount.toLong()
	LOG.debug("numberOfProcessingItems is $numberOfProcessingItems")
}  catch (RuntimeException e){
	LOG.error("An unexpected exception occurred. The exception is $e")
	message = thisOutput.send("error", "generic") 
	LOG.info("$SCRIPT_NAME is ending. $message")
	return
}

if(numberOfProcessingItems > 0){
	LOG.info("There are presently some asset links in the processing subcategory. These will be examined and moved if appropriate.")
	if(numberOfProcessingItems > 500){
		if(numberOfProcessingItems > 10000){
			LOG.warn("Too many assets")
			thisOutput.errorMap.put("tooManyAssets", "There are over 10,000 assets in the process category. This script is not designed to work with this number efficiently. The script will end as a precaution to prevent interfering with the site.")
			LOG.debug("New errorMap is $thisOutput.errorMap")
			message = thisOutput.send("error", "tooManyAssets") 
			LOG.info("$SCRIPT_NAME is ending. $message")
			return
		}
		long index = numberOfProcessingItems % 500
		if(index != 0){
			LOG.debug("The original value of index after remainder operator is $index")
			theseAssets.add(daletAPI.AssetService().getAssetsInCategory(processing.id,0.toLong(),index))
		}
		long totalPages = numberOfProcessingItems / 500
		LOG.debug("The totalPages variable set up to perform the for loop has the value $totalPages")
		for(long page; page < totalPages; page++){
			theseAssets.add(daletAPI.AssetService().getAssetsInCategory(processing.id,index,500.toLong()))
			index = index + 500
			LOG.debug("The value of index at the end of this loop iteration is $index")
		}			
	} else{
		theseAssets.add(daletAPI.AssetService().getAssetsInCategory(processing.id,0.toLong(),numberOfProcessingItems))
	}
	theseAssets = theseAssets.flatten().unique()
	LOG.info("There are currently $theseAssets.size items in the process category. Their IDs are $theseAssets.id")
} else{
	LOG.info("The process category is currently empty.")
}

/* Check if any of the assets in the process category are also in the queued categories. If any are then delete those links from the queued categories. */
List<Long> queuedIds = []
for(thisCategory in priorityCategories){
	try{queuedIds.add(daletAPI.AssetService().getAssetsInCategory(thisCategory.id,0.toLong(),thisCategory.itemsCount).id)
		queuedIds = queuedIds.flatten().unique()
	} catch(RuntimeException e){
		LOG.error("An unexpected exception occurred. The exception is $e")
		message = thisOutput.send("error", "generic") 
		LOG.info("$SCRIPT_NAME is ending. $message")
		return
	}
	for(thisId in queuedIds){
		if(theseAssets.find{it.id == thisId}){
			LOG.debug("Asset link ID $thisId was found in both the processing category and in the $thisCategory.name category. Attempting to remove the link from this queue category.")
			try{result = daletAPI.AssetService().removeLinkFromCategory(thisId, thisCategory.id)
				if(result){
					LOG.debug("Asset link ID $thisId was removed from $thisCategory.name (ID: $thisCategory.id).")
				} else{
					LOG.error("Asset link ID $thisId could not be removed from $thisCategory.name (ID: $thisCategory.id).")
				}
			} catch (RuntimeException e){
				LOG.error("An unexpected exception occurred. The exception is $e")
				message = thisOutput.send("error", "generic") 
				LOG.info("$SCRIPT_NAME is ending. $message")
				return
			}
		}				
	}
	queuedIds = []
}

/** Check if the assets in processing currently have compliance workflows running. Check their statuses and problem solve
		1) If an asset has no compliance workflow running on it then check if it is already in Programs.
			1.a) If it is then delete the link.
			1.b) If it is not then move it to the failed category.
		2) If an asset has more than one complaince workflow running then abort all but the latest one.
		3) Check the last workflow's status.
		4) Take action based on the status found. The actions are listed in the switch statement below.
**/
if(theseAssets.size > 0){	
	theseAssets.each{
		def complianceWorkflows = daletAPI.WorkflowService().getWorkflowInstancesByAssetId(it.id, false, false).findAll{it.workflowDefinitionName == "Compliance"}
		if(complianceWorkflows){
			LOG.debug("Asset ID $it.id has $complianceWorkflows.size complianceWorkflows")
			/* 2) If an asset has more than one complaince workflow running then abort all but the latest one. */
			if(complianceWorkflows.size > 1){
				LOG.warn("There are more than one Compliance workflows running on asset ID $it.id. Attempting to abort all but the last instance on this asset.")
				def lastWorkflow = complianceWorkflows[0]
				for(thisWorkflow in complianceWorkflows){
					if(lastWorkflow.startTime.time < thisWorkflow.startTime.time){
						lastWorkflow = thisWorkflow
					}
				}
				LOG.debug("The last compliance workflow's ID is $lastWorkflow.id")
				complianceWorkflows.remove(lastWorkflow)
				LOG.debug("complianceWorkflows size is now $complianceWorkflows.size. It should be 1.")
				failedAttempts = daletAPI.WorkflowService().abortWorkflowInstances(complianceWorkflows.id)
				if(!failedAttempts){
					LOG.info("All but the last Compliance workflows have been aborted.")
					complianceWorkflows.add(lastWorkflow)
				}
				else{
					/* At this time there is no plan on what to do if the script fails to abort old workflows. So, for now, it will exit on error */
					LOG.error("The attempt to abort all previous Compliance workflows failed.")
					message = thisOutput.send("error", "generic") 
					LOG.info("$SCRIPT_NAME is ending. $message")
					return
				}
			}
			/* 3) Check the last workflow's status. */
			WorkflowInstance lastCompliance = daletAPI.WorkflowService().getWorkflowInstanceById(complianceWorkflows[0].id)
			switch(lastCompliance.status.toString()){				
				case "RUNNING":
					/* If the workflow has been running for more than 10 minutes then move the link to the failed category. */
					LOG.info("The last compliance workflow running for asset $it.id is RUNNING status. Checking if the workflow was started more than ten minutes ago...")
					def currentTime = new GregorianCalendar()
					currentTime.add(Calendar.MINUTE, -10)
					LOG.debug("currentTime minus ten minutes is $currentTime.time")
					long timeLimit = lastCompliance.startTime.compareTo(currentTime)
					LOG.debug("The time ten minutes ago is $currentTime.time and the last compliance's start time is $lastCompliance.startTime.time. The comparison result is $timeLimit")
					if(timeLimit <= 0){
						LOG.info("The last compliance workflow for link ID $it.id was started over ten minutes ago.")
						(result, resultText) = moveOrRemoveLink(it.id, processing, failed)
						LOG.info("$resultText")
					} else{
						LOG.info("The last compliance workflow for link ID $it.id was started less than ten minutes ago. This link will be left alone for now.")
					}
					break
				case "SUSPENDED":
					/* If the workflow is suspended for more than 10 minutes then move it to the suspended category. */
					LOG.info("The last compliance workflow running for asset $it.id is SUSPENDED status. Checking if the workflow was started more than ten minutes ago...")
					def currentTime = new GregorianCalendar()
					currentTime.add(Calendar.MINUTE, -10)
					LOG.debug("currentTime minus ten minutes is $currentTime.time")
					long timeLimit = lastCompliance.startTime.compareTo(currentTime)
					LOG.debug("The time ten minutes ago is $currentTime.time and the last compliance's start time is $lastCompliance.startTime.time. The comparison result is $timeLimit")
					if(timeLimit <= 0){
						LOG.info("The last compliance workflow for link ID $it.id was started over ten minutes ago. Attempting to move this link to the suspended subcategory...")
						(result, resultText) = moveOrRemoveLink(it.id, processing, suspended)
						LOG.info("$resultText")
					} else{
						LOG.info("The last compliance workflow for link ID $it.id was started less than ten minutes ago. This link will be left alone for now.")
					}
					break
				case "FAILED":
					/* If the workflow is failed then move it to the failed subcategory */
					LOG.info("The last compliance workflow running for asset $it.id is FAILED status.")
					(result, resultText) = moveOrRemoveLink(it.id, processing, failed)
					LOG.info("$resultText")
					break
				case "COMPLETED":
				case "ABORTED":
					/* If the workflow is completed or aborted then delete the link. */
					LOG.info("The last compliance workflow left is either COMPLETED or ABORTED.")
					(result, resultText) = moveOrRemoveLink(it.id, processing)
					LOG.info("$resultText")
					break
				default:
					/* If none of the above are computed then this is an error and the script needs to move the link to the failed category. */
					LOG.warn("The lookup for the status of link ID $it.id's last compliance workflow resulted in an unknown value. The default action of aborting the workflow and moving the link to the failed subcategory will commence.")
					(result, resultText) = moveOrRemoveLink(it.id, processing, failed)
					LOG.info("$resultText")
					break
			}
		} else{
			LOG.debug("There are no compliance workflows running for asset ID $it.id. Checking if the asset is in the Library...")
			if(it.primaryCategoryId != libraryId){
				LOG.info("This parent asset is not in the Library and does not have a compliance workflow running. Moving it to the failed category.")
				(result, resultText) = moveOrRemoveLink(it.id, processing, failed)
				LOG.info("$resultText")
			} else{
				LOG.info("This parent asset is in the Library and is assumed completed. The link will be deleted.")
				(result, resultText) = moveOrRemoveLink(it.id, processing)
				LOG.info("$resultText")
			}
		}
	}
	LOG.info("Finished examining asset links in the processing category.")
}

/* Get the updated assets in the process category. If the number is less than the input maximum limit then move enough assets from the queued category into the processing category to match the limit */
try{numberOfProcessingItems = daletAPI.CategoryService().getCategoriesByIds([processing.id])[0].itemsCount.toLong()
	LOG.debug("numberOfProcessingItems is $numberOfProcessingItems")
}  catch (RuntimeException e){
	LOG.error("An unexpected exception occurred. The exception is $e")
	message = thisOutput.send("error", "generic") 
	LOG.info("$SCRIPT_NAME is ending. $message")
	return
}
if(numberOfProcessingItems < maxNumber){
	LOG.info("There are less than the maximum number of links in the processing category. Attempting to add more title links...")
	/* Look for the priority categories and queue links from them in this priority: urgent > high > normal > low > queued */
	long maxResults = maxNumber - numberOfProcessingItems
	if(urgent != null){
		def thisResult
		try{thisResult = daletAPI.AssetService().getAssetsInCategory(urgent.id,0.toLong(),maxResults).id
			if(thisResult.size > 0){
				thisResult.each{
					(result, resultText) = moveOrRemoveLink(it, urgent, processing)
					LOG.info("$resultText")
				}
				maxResults = maxResults - thisResult.size
			} else{
				LOG.info("There are no asset links in category $urgent.name (ID: $urgent.id) to queue.")
			}
		} catch(RuntimeException e){
			LOG.error("An unexpected exception occurred. The exception is $e")
			message = thisOutput.send("error", "generic") 
			LOG.info("$SCRIPT_NAME is ending. $message")
			return
		}		
	}
	if(maxResults > 0){
		if(high != null){
			def thisResult
			try{thisResult = daletAPI.AssetService().getAssetsInCategory(high.id,0.toLong(),maxResults).id
				if(thisResult.size > 0){
					thisResult.each{
						(result, resultText) = moveOrRemoveLink(it, high, processing)
						LOG.info("$resultText")
					}
					maxResults = maxResults - thisResult.size
				} else{
					LOG.info("There are no asset links in category $high.name (ID: $high.id) to queue.")
				}
			} catch(RuntimeException e){
				LOG.error("An unexpected exception occurred. The exception is $e")
				message = thisOutput.send("error", "generic") 
				LOG.info("$SCRIPT_NAME is ending. $message")
				return
			}
		}
	}
	if(maxResults > 0){
		if(normal != null){
			def thisResult
			try{thisResult = daletAPI.AssetService().getAssetsInCategory(normal.id,0.toLong(),maxResults).id
				if(thisResult.size > 0){
					thisResult.each{
						(result, resultText) = moveOrRemoveLink(it, normal, processing)
						LOG.info("$resultText")
					}
					maxResults = maxResults - thisResult.size
				} else{
					LOG.info("There are no asset links in category $normal.name (ID: $normal.id) to queue.")
				}
			} catch(RuntimeException e){
				LOG.error("An unexpected exception occurred. The exception is $e")
				message = thisOutput.send("error", "generic") 
				LOG.info("$SCRIPT_NAME is ending. $message")
				return
			}
		}
	}
	if(maxResults > 0){
		if(low != null){
			def thisResult
			try{thisResult = daletAPI.AssetService().getAssetsInCategory(low.id,0.toLong(),maxResults).id
				if(thisResult.size > 0){
					thisResult.each{
						(result, resultText) = moveOrRemoveLink(it, low, processing)
						LOG.info("$resultText")
					}
					maxResults = maxResults - thisResult.size
				} else{
					LOG.info("There are no asset links in category $low.name (ID: $low.id) to queue.")
				}
			} catch(RuntimeException e){
				LOG.error("An unexpected exception occurred. The exception is $e")
				message = thisOutput.send("error", "generic") 
				LOG.info("$SCRIPT_NAME is ending. $message")
				return
			}
		}
	}
	if(maxResults > 0){
		if(queued != null){
			def thisResult
			try{thisResult = daletAPI.AssetService().getAssetsInCategory(queued.id,0.toLong(),maxResults).id
				if(thisResult.size > 0){
					thisResult.each{
						(result, resultText) = moveOrRemoveLink(it, queued, processing)
						LOG.info("$resultText")
					}
					maxResults = maxResults - thisResult.size
				} else{
					LOG.info("There are no asset links in category $queued.name (ID: $queued.id) to queue.")
				}
			} catch(RuntimeException e){
				LOG.error("An unexpected exception occurred. The exception is $e")
				message = thisOutput.send("error", "generic") 
				LOG.info("$SCRIPT_NAME is ending. $message")
				return
			}
		}
	}
} else {
	LOG.info("The maximum amount of allowable links are in the processing category. No further links will be added.")
}

/** Check the suspended category to see which asset links are still running compliance.
	1) Assets no longer running compiance will be checked if they are in the Library.
		1.a) If they are then the link will be deleted
		1.b) If they are not then they will be moved to the failed category.
	2) Assets running compliance will be checked for the time limit of seven days
		2.a) If they have been running for less than seven days then they will be added to a message sent for reporting to the users.
		2.b) If they have been running for seven days or more then they will be moved to the failed category.
**/
long numberOfSuspendedItems
theseAssets = [] // Reset theseAssets to reuse the list
try{numberOfSuspendedItems = daletAPI.CategoryService().getCategoriesByIds([suspended.id])[0].itemsCount.toLong()
	LOG.debug("numberOfSuspendedItems is $numberOfSuspendedItems")
}  catch (RuntimeException e){
	LOG.error("An unexpected exception occurred. The exception is $e")
	message = thisOutput.send("error", "generic") 
	LOG.info("$SCRIPT_NAME is ending. $message")
	return
}

if(numberOfSuspendedItems > 0){
	LOG.info("There are presently some asset links in the processing subcategory. These will be examined and moved if appropriate.")
	if(numberOfSuspendedItems > 500){
		if(numberOfSuspendedItems > 10000){
			LOG.warn("Too many assets")
			thisOutput.errorMap.put("tooManyAssets", "There are over 10,000 assets in the suspended category. This script is not designed to work with this number efficiently. The script will end as a precaution to prevent interfering with the site.")
			LOG.debug("New errorMap is $thisOutput.errorMap")
			message = thisOutput.send("error", "tooManyAssets") 
			LOG.info("$SCRIPT_NAME is ending. $message")
			return
		}
		long index = numberOfSuspendedItems % 500
		if(index != 0){
			LOG.debug("The original value of index after remainder operator is $index")
			theseAssets.add(daletAPI.AssetService().getAssetsInCategory(suspended.id,0.toLong(),index))
		}
		long totalPages = numberOfSuspendedItems / 500
		LOG.debug("The totalPages variable set up to perform the for loop has the value $totalPages")
		for(long page; page < totalPages; page++){
			theseAssets.add(daletAPI.AssetService().getAssetsInCategory(suspended.id,index,500.toLong()))
			index = index + 500
			LOG.debug("The value of index at the end of this loop iteration is $index")
		}			
	} else{
		theseAssets.add(daletAPI.AssetService().getAssetsInCategory(suspended.id,0.toLong(),numberOfSuspendedItems))
	}
	theseAssets = theseAssets.flatten().unique()
	LOG.info("There are currently $theseAssets.size items in the process category. Their IDs are $theseAssets.id")
} else{
	LOG.info("The suspended category is currently empty.")
}

if(theseAssets.size > 0){
	theseAssets.each{
		def complianceWorkflows = daletAPI.WorkflowService().getWorkflowInstancesByAssetId(it.id, false, false).findAll{it.workflowDefinitionName == "Compliance"}
		if(complianceWorkflows){
			LOG.debug("Asset ID $it.id has $complianceWorkflows.size complianceWorkflows")
			/* 1) If an asset has more than one complaince workflow running then abort all but the latest one. */
			if(complianceWorkflows.size > 1){
				LOG.warn("There are more than one Compliance workflows running on asset ID $it.id. Attempting to abort all but the last instance on this asset.")
				def lastWorkflow = complianceWorkflows[0]
				for(thisWorkflow in complianceWorkflows){
					if(lastWorkflow.startTime.time < thisWorkflow.startTime.time){
						lastWorkflow = thisWorkflow
					}
				}
				LOG.debug("The last compliance workflow's ID is $lastWorkflow.id")
				complianceWorkflows.remove(lastWorkflow)
				LOG.debug("complianceWorkflows size is now $complianceWorkflows.size. It should be 1.")
				failedAttempts = daletAPI.WorkflowService().abortWorkflowInstances(complianceWorkflows.id)
				if(!failedAttempts){
					LOG.info("All but the last Compliance workflows have been aborted.")
					complianceWorkflows.add(lastWorkflow)
				}
				else{
					/* At this time there is no plan on what to do if the script fails to abort old workflows. So, for now, it will exit on error */
					LOG.error("The attempt to abort all previous Compliance workflows failed.")
					message = thisOutput.send("error", "generic") 
					LOG.info("$SCRIPT_NAME is ending. $message")
					return
				}
			}
			/* 2) Check the last workflow's status. */
			WorkflowInstance lastCompliance = daletAPI.WorkflowService().getWorkflowInstanceById(complianceWorkflows[0].id)
			switch(lastCompliance.status.toString()){				
				case "RUNNING":
					/* Move to the processing category. */
					LOG.info("The last compliance workflow running for asset $it.id is RUNNING status. Moving to the processing category.")
					(result, resultText) = moveOrRemoveLink(it.id, suspended, processing)
					LOG.info("$resultText")
					break
				case "SUSPENDED":
					/* If the workflow is suspended for more than seven days then delete the link. Otherwise queue it in a message to be send to the users. */
					LOG.info("The last compliance workflow running for asset $it.id is SUSPENDED status. Checking if the workflow was started at least seven days ago...")
					def currentTime = new GregorianCalendar()
					currentTime.add(Calendar.DAY, -7)
					LOG.debug("currentTime minus seven days is $currentTime.time")
					long timeLimit = lastCompliance.startTime.compareTo(currentTime)
					LOG.debug("The time seven days ago is $currentTime.time and the last compliance's start time is $lastCompliance.startTime.time. The comparison result is $timeLimit")
					if(timeLimit <= 0){
						LOG.info("The last compliance workflow for link ID $it.id was started at least seven days ago. Attempting to remove this link...")
						(result, resultText) = moveOrRemoveLink(it.id, suspended)
						LOG.info("$resultText")
					} else{
						LOG.info("The last compliance workflow for link ID $it.id was started less than seven days ago. Users will be notified that this compliance is SUSPENDED and will be deleted in seven days.")
						/* ADD CODE HERE */
					}
					break
				case "FAILED":
					/* If the workflow is failed then abort the link's workflow and move it to the failed subcategory */
					LOG.info("The last compliance workflow running for asset $it.id is FAILED status. Attempting to abort it...")
					def failedRequests = []
					try{
						failedRequests = daletAPI.WorkflowService().abortWorkflowInstances([lastCompliance.id])
					} catch (RuntimeException e){
						LOG.info("Exception occurred: $e")
					}
					if(failedRequests.size() > 0){
						LOG.error("Workflow ID $lastCompliance.id for Link ID $it.id could not be aborted.")
					} else{
						LOG.info("Workflow ID $lastCompliance.id was aborted for Link ID $it.id")
					}
					(result, resultText) = moveOrRemoveLink(it.id, suspended, failed)
					LOG.info("$resultText")
					break
				case "COMPLETED":
				case "ABORTED":
					/* If the workflow is completed or aborted then delete the link. */
					LOG.info("The last compliance workflow left is either COMPLETED or ABORTED.")
					(result, resultText) = moveOrRemoveLink(it.id, suspended)
					LOG.info("$resultText")
					break
				default:
					/* If none of the above are computed then this is an error and the script needs to abort the workflow and move the link to the failed category. */
					LOG.warn("The lookup for the status of link ID $it.id's last compliance workflow resulted in an unknown value. The default action of aborting the workflow and moving the link to the failed subcategory will commence.")
					def failedRequests = []
					try{
						failedRequests = daletAPI.WorkflowService().abortWorkflowInstances([lastCompliance.id])
					} catch (RuntimeException e){
						LOG.info("Exception occurred: $e")
					}
					if(failedRequests.size() > 0){
						LOG.error("Workflow ID $lastCompliance.id for Link ID $it.id could not be aborted.")
					} else{
						LOG.info("Workflow ID $lastCompliance.id was aborted for Link ID $it.id")
					}
					LOG.info("Now attempting to move the link to the failed category...")
					(result, resultText) = moveOrRemoveLink(it.id, suspended, failed)
					LOG.info("$resultText")
					break
			}
		} else{
			LOG.debug("There are no compliance workflows running for asset ID $it.id. Checking if the asset is in the Library...")
			if(it.primaryCategoryId != libraryId){
				LOG.info("This parent asset is not in the Library and does not have a compliance workflow running. Moving it to the failed category.")
				(result, resultText) = moveOrRemoveLink(it.id, suspended, failed)
				LOG.info("$resultText")
			} else{
				LOG.info("This parent asset is in the Library and is assumed completed. The link will be deleted.")
				(result, resultText) = moveOrRemoveLink(it.id, suspended)
				LOG.info("$resultText")
			}
		}
	}
	LOG.info("Finished examining asset links in the suspended category.")
}

/** Check the failed category to see which asset links are still running compliance.
	1) Assets no longer running compiance will be checked if they are in the Library.
		1.a) If they are then the link will be deleted
		1.b) If they are not then run Compliance Recovery.
			1.b.a) If Compliance Recovery cannot be run then delete the link and send a message to the users
	2) Assets running compliance will be checked for start time of the workflow.
		2.a) If the start time is less than one thirty minutes then retry the workflow
		2.b) If the start time is equal or more than thirty minutes then abort the workfow and run the Compliance Recovery workflow on the title.
**/
long numberOfFailedItems
theseAssets = [] // Reset theseAssets to reuse the list
try{numberOfFailedItems = daletAPI.CategoryService().getCategoriesByIds([failed.id])[0].itemsCount.toLong()
	LOG.debug("numberOfFailedItems is $numberOfFailedItems")
}  catch (RuntimeException e){
	LOG.error("An unexpected exception occurred. The exception is $e")
	message = thisOutput.send("error", "generic") 
	LOG.info("$SCRIPT_NAME is ending. $message")
	return
}

if(numberOfFailedItems > 0){
	LOG.info("There are presently some asset links in the processing subcategory. These will be examined and moved if appropriate.")
	if(numberOfFailedItems > 500){
		if(numberOfFailedItems > 10000){
			LOG.warn("Too many assets")
			thisOutput.errorMap.put("tooManyAssets", "There are over 10,000 assets in the failed category. This script is not designed to work with this number efficiently. The script will end as a precaution to prevent interfering with the site.")
			LOG.debug("New errorMap is $thisOutput.errorMap")
			message = thisOutput.send("error", "tooManyAssets") 
			LOG.info("$SCRIPT_NAME is ending. $message")
			return
		}
		long index = numberOfFailedItems % 500
		if(index != 0){
			LOG.debug("The original value of index after remainder operator is $index")
			theseAssets.add(daletAPI.AssetService().getAssetsInCategory(failed.id,0.toLong(),index))
		}
		long totalPages = numberOfFailedItems / 500
		LOG.debug("The totalPages variable set up to perform the for loop has the value $totalPages")
		for(long page; page < totalPages; page++){
			theseAssets.add(daletAPI.AssetService().getAssetsInCategory(failed.id,index,500.toLong()))
			index = index + 500
			LOG.debug("The value of index at the end of this loop iteration is $index")
		}			
	} else{
		theseAssets.add(daletAPI.AssetService().getAssetsInCategory(failed.id,0.toLong(),numberOfFailedItems))
	}
	theseAssets = theseAssets.flatten().unique()
	LOG.info("There are currently $theseAssets.size items in the process category. Their IDs are $theseAssets.id")
} else{
	LOG.info("The failed category is currently empty.")
}

if(theseAssets.size > 0){
	theseAssets.each{
		def complianceWorkflows = daletAPI.WorkflowService().getWorkflowInstancesByAssetId(it.id, false, false).findAll{it.workflowDefinitionName == "Compliance"}
		if(complianceWorkflows){
			LOG.debug("Asset ID $it.id has $complianceWorkflows.size complianceWorkflows")
			/* If an asset has more than one complaince workflow running then abort all but the latest one. */
			if(complianceWorkflows.size > 1){
				LOG.warn("There are more than one Compliance workflows running on asset ID $it.id. Attempting to abort all but the last instance on this asset.")
				def lastWorkflow = complianceWorkflows[0]
				for(thisWorkflow in complianceWorkflows){
					if(lastWorkflow.startTime.time < thisWorkflow.startTime.time){
						lastWorkflow = thisWorkflow
					}
				}
				LOG.debug("The last compliance workflow's ID is $lastWorkflow.id")
				complianceWorkflows.remove(lastWorkflow)
				LOG.debug("complianceWorkflows size is now $complianceWorkflows.size. It should be 1.")
				failedAttempts = daletAPI.WorkflowService().abortWorkflowInstances(complianceWorkflows.id)
				if(!failedAttempts){
					LOG.info("All but the last Compliance workflows have been aborted.")
					complianceWorkflows.add(lastWorkflow)
				}
				else{
					/* At this time there is no plan on what to do if the script fails to abort old workflows. So, for now, it will exit on error */
					LOG.error("The attempt to abort all previous Compliance workflows failed.")
					message = thisOutput.send("error", "generic") 
					LOG.info("$SCRIPT_NAME is ending. $message")
					return
				}
			}
			
			/* Check the last workflow's status. */
			LOG.info("The following links are in the failed process category. The next actions will reflect that.")
			WorkflowInstance lastCompliance = daletAPI.WorkflowService().getWorkflowInstanceById(complianceWorkflows[0].id)
			switch(lastCompliance.status.toString()){				
				case "RUNNING":
					/* Check links in RUNNING status for start time. If it is more than 30 minutes then abort the workflow, delete the link, and run the Compliance Recovery workflow
					*/
					LOG.info("The last compliance workflow running for asset $it.id is RUNNING status. Checking if the workflow was started more than thirty minutes ago...")
					def currentTime = new GregorianCalendar()
					currentTime.add(Calendar.MINUTE, -30)
					LOG.debug("currentTime minus thirty minutes is $currentTime.time")
					long timeLimit = lastCompliance.startTime.compareTo(currentTime)
					LOG.debug("The time thirty minutes ago was $currentTime.time and the last compliance's start time is $lastCompliance.startTime.time. The comparison result is $timeLimit")
					if(timeLimit <= 0){
						LOG.info("The last compliance workflow for link ID $it.id was started at least minutes ago.")
						def failedRequests = []
						try{
							failedRequests = daletAPI.WorkflowService().abortWorkflowInstances([lastCompliance.id])
						} catch (RuntimeException e){
							LOG.info("Exception occurred: $e")
						}
						if(failedRequests.size() > 0){
							LOG.error("Workflow ID $lastCompliance.id for Link ID $it.id could not be aborted.")
						} else{
							LOG.info("Workflow ID $lastCompliance.id was aborted for Link ID $it.id")
						}	
					} else{
						LOG.info("The last compliance workflow for link ID $it.id was started less than thirty minutes ago. This link will be left alone for now.")
					}
					(result, resultText) = moveOrRemoveLink(it.id, failed)
					LOG.info("$resultText")
					(result, resultText) = runWorkflow(it.id, workflowName)
					break
				case "SUSPENDED":
					/* Move to the suspended category. */
					LOG.info("The last compliance workflow running for asset $it.id is SUSPENDED status. Moving to the suspended category.")
					(result, resultText) = moveOrRemoveLink(it.id, failed, suspended)
					LOG.info("$resultText")
					break
				case "FAILED":
					/* If the workflow is failed then abort the link's workflow, delete the link, and run the Compliance Recovery workflow */
					def failedRequests = []
					try{
						failedRequests = daletAPI.WorkflowService().abortWorkflowInstances([lastCompliance.id])
					} catch (RuntimeException e){
						LOG.info("Exception occurred: $e")
					}
					if(failedRequests.size() > 0){
						LOG.error("Workflow ID $lastCompliance.id for Link ID $it.id could not be aborted.")
					} else{
						LOG.info("Workflow ID $lastCompliance.id was aborted for Link ID $it.id")
					}
					(result, resultText) = moveOrRemoveLink(it.id, failed)
					LOG.info("$resultText")
					(result, resultText) = runWorkflow(it.id, workflowName)
					break
				case "COMPLETED":
				case "ABORTED":
					/* If the workflow is completed or aborted then delete the link */
					LOG.info("The last compliance workflow left is either COMPLETED or ABORTED.")
					(result, resultText) = moveOrRemoveLink(it.id, failed)
					LOG.info("$resultText")
					break
				default:
					/* If none of the above are computed then this is an error and the script needs to abort the workflow, delete the link, and send a message to the users. */
					def failedRequests = []
					try{
						failedRequests = daletAPI.WorkflowService().abortWorkflowInstances([lastCompliance.id])
					} catch (RuntimeException e){
						LOG.info("Exception occurred: $e")
					}
					if(failedRequests.size() > 0){
						LOG.error("Workflow ID $lastCompliance.id for Link ID $it.id could not be aborted.")
					} else{
						LOG.info("Workflow ID $lastCompliance.id was aborted for Link ID $it.id")
					}
					(result, resultText) = moveOrRemoveLink(it.id, failed)
					LOG.info("$resultText")
					/** ADD SEND MESSAGE CALL HERE **/
					break
			}
		} else{
			LOG.debug("There are no compliance workflows running for asset ID $it.id. Since it is in the failed category this asset link will be deleted.")
			(result, resultText) = moveOrRemoveLink(it.id, failed)
			LOG.info("$resultText")
		}
	}
	LOG.info("Finished examining asset links in the failed category.")
}

/* Post the output and end */
message = thisOutput.send()
LOG.info("$SCRIPT_NAME is ending. $message")
