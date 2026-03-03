import React from 'react';
import { L3TaskTree, GoalTree } from '@/lib/types';
import { Badge } from '@/components/ui/badge';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { CheckCircle2, Circle, ArrowRight, Loader2, AlertCircle, XCircle } from 'lucide-react';

function StatusIcon({ status }: { status: any }) {
  if (status === 'Completed') return <CheckCircle2 className="w-4 h-4 text-green-500" />;
  if (status === 'InProgress') return <Loader2 className="w-4 h-4 text-blue-500 animate-spin" />;
  if (typeof status === 'object' && status !== null) {
    if ('Blocked' in status) return <AlertCircle className="w-4 h-4 text-orange-500" />;
    if ('Failed' in status) return <XCircle className="w-4 h-4 text-red-500" />;
  }
  if (status === 'Cancelled') return <XCircle className="w-4 h-4 text-gray-500" />;
  return <Circle className="w-4 h-4 text-gray-400" />;
}

function StatusBadge({ status }: { status: any }) {
  if (status === 'Completed') return <Badge variant="outline" className="text-green-500 border-green-500/30">Completed</Badge>;
  if (status === 'InProgress') return <Badge variant="outline" className="text-blue-500 border-blue-500/30">In Progress</Badge>;
  if (typeof status === 'object' && status !== null) {
    if ('Blocked' in status) return <Badge variant="outline" className="text-orange-500 border-orange-500/30">Blocked: {status.Blocked}</Badge>;
    if ('Failed' in status) return <Badge variant="outline" className="text-red-500 border-red-500/30">Failed: {status.Failed}</Badge>;
  }
  if (status === 'Cancelled') return <Badge variant="outline" className="text-gray-500 border-gray-500/30">Cancelled</Badge>;
  return <Badge variant="outline" className="text-gray-400 border-gray-400/30">Pending</Badge>;
}

function TaskNode({ node }: { node: L3TaskTree }) {
  const { task, children } = node;
  return (
    <div className="pl-6 border-l border-muted-foreground/20 ml-3 relative mt-4">
      <div className="absolute w-6 h-[1px] bg-muted-foreground/20 left-0 top-3"></div>
      <div className="flex flex-col gap-2 bg-muted/10 p-3 rounded-md border border-muted/20 hover:bg-muted/30 transition-colors">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <StatusIcon status={task.status} />
            <span className="font-semibold text-sm">{task.title}</span>
          </div>
          <StatusBadge status={task.status} />
        </div>
        
        {task.description && task.description !== task.title && (
          <p className="text-xs text-muted-foreground pl-6 line-clamp-2">{task.description}</p>
        )}
        
        {task.result_summary && (
          <div className="pl-6 mt-1">
            <div className="bg-background/50 rounded p-2 border border-muted text-xs text-muted-foreground">
              <span className="font-medium text-foreground mr-1">Result:</span>
              {task.result_summary}
            </div>
          </div>
        )}

        {task.progress > 0 && task.progress < 1 && (
          <div className="pl-6 w-full max-w-xs mt-1">
            <div className="h-1.5 w-full bg-muted rounded-full overflow-hidden">
              <div 
                className="h-full bg-blue-500 transition-all duration-500" 
                style={{ width: `${task.progress * 100}%` }}
              />
            </div>
          </div>
        )}
      </div>

      {children && children.length > 0 && (
        <div className="mt-2">
          {children.map((child, idx) => (
            <TaskNode key={child.task.task_id || idx} node={child} />
          ))}
        </div>
      )}
    </div>
  );
}

export function TaskTreeViewer({ trees }: { trees: GoalTree[] }) {
  if (!trees || trees.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center p-8 text-center border rounded-lg bg-muted/5 border-dashed">
        <p className="text-sm text-muted-foreground">No L3 Task Trees found for this context.</p>
        <p className="text-xs text-muted-foreground mt-1">Goals decomposed into L3Tasks will appear here.</p>
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-6">
      {trees.map((tree, i) => (
        <Card key={tree.goal.id || i} className="overflow-hidden">
          <CardHeader className="bg-muted/30 pb-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <Badge className="bg-purple-500 hover:bg-purple-600">Goal (L3)</Badge>
                <CardTitle className="text-lg">{tree.goal.content}</CardTitle>
              </div>
            </div>
            <div className="flex gap-2 text-xs text-muted-foreground mt-2">
              <span className="font-mono">{tree.goal.id.substring(0, 8)}</span>
              <span>•</span>
              <span>{new Date(tree.goal.transaction_time).toLocaleString()}</span>
            </div>
          </CardHeader>
          <CardContent className="p-4 pt-2">
            {tree.tasks.length > 0 ? (
              <div className="-ml-3 mt-2">
                {tree.tasks.map((task, j) => (
                  <TaskNode key={task.task.task_id || j} node={task} />
                ))}
              </div>
            ) : (
              <div className="py-4 pl-4 text-sm text-muted-foreground italic">
                This goal has not been decomposed into tasks yet.
              </div>
            )}
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
